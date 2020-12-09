{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module SN.Ground.Fake
    ( UploadProcess(..)
    , DownloadProcess(..)
    , GroundAction(..)
    , ActionExpectation
    , (|>>)
    , FakeGround
    , checkpoint
    , withFakeGround
    ) where

import Data.IORef
import Data.Int
import Data.Semigroup
import GHC.Stack
import Test.Hspec
import Type.Reflection
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import SN.Data.HEq1
import SN.Ground

data UploadProcess = UploadProcess FileInfo [BS.ByteString] (Either GroundError (Bool, Maybe GroundError))
    deriving (Show, Eq)

data DownloadProcess = DownloadProcess Text.Text [BS.ByteString]
    deriving (Show, Eq)

data GroundAction a where
    GroundPerform :: Action a -> GroundAction (Either GroundError a)
    GroundGenerateBytes :: Int -> GroundAction BS.ByteString
    GroundUpload :: Text.Text -> Text.Text -> Reference Article -> Reference User -> GroundAction (Either GroundError UploadProcess)
    GroundDownload :: Reference FileInfo -> GroundAction (Either GroundError DownloadProcess)
deriving instance Show (GroundAction a)
deriving instance Eq (GroundAction a)

instance HEq1 GroundAction where
    heq (GroundPerform ba) (GroundPerform bb)
        | Just Refl <- heq ba bb = Just Refl
    heq a@(GroundGenerateBytes {}) b@(GroundGenerateBytes {})
        | a == b = Just Refl
    heq a@(GroundUpload {}) b@(GroundUpload {})
        | a == b = Just Refl
    heq a@(GroundDownload {}) b@(GroundDownload {})
        | a == b = Just Refl
    heq _ _ = Nothing

data ActionExpectation
    = forall a. Show a => ActionExpectation
        (GroundAction a)
        a
        (forall t u. (HasCallStack => t -> u) -> (t -> u))

instance Show ActionExpectation where
    showsPrec d (ActionExpectation action result _) =
        showParen (d > 0) $
        showsPrec 1 action . showString " |>> " . showsPrec 1 result

infix 1 |>>
class IsActionExpectation a r | a -> r where
    (|>>) :: (Show r, HasCallStack) => a -> r -> ActionExpectation

instance IsActionExpectation (GroundAction a) a where
    action |>> result = ActionExpectation action result id

instance IsActionExpectation (Action a) (Either GroundError a) where
    action |>> result = ActionExpectation (GroundPerform action) result id

data Context = Context
    [ActionExpectation]
    (forall t u. (HasCallStack => t -> u) -> (t -> u))

newtype FakeGround = FakeGround (IORef Context)

checkpoint :: HasCallStack => FakeGround -> [ActionExpectation] -> IO ()
checkpoint (FakeGround pBuffer) next = do
    Context prev _ <- readIORef pBuffer
    case prev of
        [] -> return ()
        ActionExpectation _ _ withStack : _ -> withStack expectationFailure $
            "Actions not performed:" ++ concatMap (\a -> "\n\t" ++ show a) prev
    writeIORef pBuffer $ Context next id

withFakeGround
    :: HasCallStack
    => (FakeGround -> Ground -> IO r)
    -> IO r
withFakeGround body = do
    context <- FakeGround <$> newIORef (Context [] id)
    result <- body context $ Ground
        { groundPerform = \action -> do
            expectAction context (GroundPerform action) return
        , groundGenerateBytes = \len -> do
            expectAction context (GroundGenerateBytes len) return
        , groundUpload = fakeGroundUpload context
        , groundDownload = fakeGroundDownload context
        }
    checkpoint context []
    return result

fakeGroundUpload
    :: FakeGround
    -> Text.Text
    -> Text.Text
    -> Reference Article
    -> Reference User
    -> (FileInfo -> IO (Upload r))
    -> IO (Either GroundError r)
fakeGroundUpload context name mimeType articleRef userRef uploader = do
    expectAction context (GroundUpload name mimeType articleRef userRef) $ \case
        Left err -> return $ Left err
        Right (UploadProcess finfo chunks uend) -> do
            driveUploader chunks uend $ uploader finfo
  where
    driveUploader
        :: HasCallStack
        => [BS.ByteString]
        -> Either GroundError (Bool, Maybe GroundError)
        -> IO (Upload r)
        -> IO (Either GroundError r)
    driveUploader [] uend func = do
        case uend of
            Left err -> return $ Left err
            Right (accept, merr) -> do
                ur <- func
                r <- case accept of
                    True -> do
                        AnUpload ur `shouldBe` AnUpload (UploadFinish ())
                        let UploadFinish r = ur
                        return r
                    False -> do
                        AnUpload ur `shouldBe` AnUpload (UploadAbort ())
                        let UploadAbort r = ur
                        return r
                case merr of
                    Nothing -> return $ Right r
                    Just err -> return $ Left err
    driveUploader (x : xs) uend func = do
        ur <- func
        AnUpload ur `shouldBe` AnUpload (UploadChunk x undefined)
        let UploadChunk _ next = ur
        driveUploader xs uend next

fakeGroundDownload
    :: FakeGround
    -> Reference FileInfo
    -> (GroundError -> IO r)
    -> (Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
    -> IO r
fakeGroundDownload context fileRef onError onStream = do
    expectAction context (GroundDownload fileRef) $ \case
        Left err -> onError err
        Right (DownloadProcess ftype chunks) -> do
            let fsize = getSum $ foldMap (Sum . fromIntegral . BS.length) chunks
            onStream fsize ftype $ \sink -> do
                mapM_ sink chunks

expectAction :: FakeGround -> GroundAction a -> (HasCallStack => a -> IO b) -> IO b
expectAction (FakeGround pBuffer) action cont = do
    Context buffer withStackOuter <- readIORef pBuffer
    case buffer of
        ActionExpectation expAction result withStack : rest -> do
            case heq expAction action of
                Just Refl -> do
                    writeIORef pBuffer $ Context rest withStackOuter
                    withStack cont result
                Nothing -> do
                    withStack shouldBe (AnAction action) (AnAction expAction)
                    fail "shouldn't reach there"
        [] -> do
            withStackOuter expectationFailure $ "Unexpected action: " ++ show action
            fail "shouldn't reach there"

data AnAction =
    forall a. AnAction (GroundAction a)

instance Show AnAction where
    showsPrec d (AnAction x) = showsPrec d x

instance Eq AnAction where
    _ == _ = False

data AnUpload = forall a. AnUpload (Upload a)

instance Show AnUpload where
    showsPrec d (AnUpload (UploadChunk chunk _)) = showParen (d > 10) $
        showString "UploadChunk " . showsPrec 11 chunk . showString " <<continuation>>"
    showsPrec d (AnUpload (UploadFinish _)) = showParen (d > 10) $
        showString "UploadFinish <<result>>"
    showsPrec d (AnUpload (UploadAbort _)) = showParen (d > 10) $
        showString "UploadAbort <<result>>"

instance Eq AnUpload where
    (==) (AnUpload (UploadChunk ca _)) (AnUpload (UploadChunk cb _)) = ca == cb
    (==) (AnUpload (UploadFinish _)) (AnUpload (UploadFinish _)) = True
    (==) (AnUpload (UploadAbort _)) (AnUpload (UploadAbort _)) = True
    (==) _ _ = False
