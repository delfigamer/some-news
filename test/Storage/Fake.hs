{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Storage.Fake
    ( UploadProcess(..)
    , DownloadProcess(..)
    , StorageAction(..)
    , ActionExpectation
    , (|>>)
    , FakeStorage
    , checkpoint
    , withFakeStorage
    ) where

import Data.IORef
import Data.Int
import Data.Semigroup
import GHC.Stack
import Test.Hspec
import Type.Reflection
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import HEq1
import Storage

data UploadProcess = UploadProcess FileInfo [BS.ByteString] (Either StorageError (Bool, Maybe StorageError))
    deriving (Show, Eq)

data DownloadProcess = DownloadProcess Text.Text [BS.ByteString]
    deriving (Show, Eq)

data StorageAction a where
    StoragePerform :: Action a -> StorageAction (Either StorageError a)
    StorageGenerateBytes :: Int -> StorageAction BS.ByteString
    StorageUpload :: Text.Text -> Text.Text -> Reference Article -> Reference User -> StorageAction (Either StorageError UploadProcess)
    StorageDownload :: Reference FileInfo -> StorageAction (Either StorageError DownloadProcess)
deriving instance Show (StorageAction a)
deriving instance Eq (StorageAction a)

instance HEq1 StorageAction where
    heq (StoragePerform ba) (StoragePerform bb)
        | Just Refl <- heq ba bb = Just Refl
    heq a@(StorageGenerateBytes {}) b@(StorageGenerateBytes {})
        | a == b = Just Refl
    heq a@(StorageUpload {}) b@(StorageUpload {})
        | a == b = Just Refl
    heq a@(StorageDownload {}) b@(StorageDownload {})
        | a == b = Just Refl
    heq _ _ = Nothing

data ActionExpectation
    = forall a. Show a => ActionExpectation
        (StorageAction a)
        a
        (forall t u. (HasCallStack => t -> u) -> (t -> u))

instance Show ActionExpectation where
    showsPrec d (ActionExpectation action result _) =
        showParen (d > 0) $
        showsPrec 1 action . showString " |>> " . showsPrec 1 result

infix 1 |>>
class IsActionExpectation a r | a -> r where
    (|>>) :: (Show r, HasCallStack) => a -> r -> ActionExpectation

instance IsActionExpectation (StorageAction a) a where
    action |>> result = ActionExpectation action result id

instance IsActionExpectation (Action a) (Either StorageError a) where
    action |>> result = ActionExpectation (StoragePerform action) result id

data Context = Context
    [ActionExpectation]
    (forall t u. (HasCallStack => t -> u) -> (t -> u))

newtype FakeStorage = FakeStorage (IORef Context)

checkpoint :: HasCallStack => FakeStorage -> [ActionExpectation] -> IO ()
checkpoint (FakeStorage pBuffer) next = do
    Context prev _ <- readIORef pBuffer
    case prev of
        [] -> return ()
        ActionExpectation _ _ withStack : _ -> withStack expectationFailure $
            "Actions not performed:" ++ concatMap (\a -> "\n\t" ++ show a) prev
    writeIORef pBuffer $ Context next id

withFakeStorage
    :: HasCallStack
    => (FakeStorage -> Storage -> IO r)
    -> IO r
withFakeStorage body = do
    context <- FakeStorage <$> newIORef (Context [] id)
    result <- body context $ Storage
        { storagePerform = \action -> do
            expectAction context (StoragePerform action) return
        , storageGenerateBytes = \len -> do
            expectAction context (StorageGenerateBytes len) return
        , storageUpload = fakeStorageUpload context
        , storageDownload = fakeStorageDownload context
        }
    checkpoint context []
    return result

fakeStorageUpload
    :: FakeStorage
    -> Text.Text
    -> Text.Text
    -> Reference Article
    -> Reference User
    -> (FileInfo -> IO (Upload r))
    -> IO (Either StorageError r)
fakeStorageUpload context name mimeType articleRef userRef uploader = do
    expectAction context (StorageUpload name mimeType articleRef userRef) $ \case
        Left err -> return $ Left err
        Right (UploadProcess finfo chunks uend) -> do
            driveUploader chunks uend $ uploader finfo
  where
    driveUploader
        :: HasCallStack
        => [BS.ByteString]
        -> Either StorageError (Bool, Maybe StorageError)
        -> IO (Upload r)
        -> IO (Either StorageError r)
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

fakeStorageDownload
    :: FakeStorage
    -> Reference FileInfo
    -> (StorageError -> IO r)
    -> (Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
    -> IO r
fakeStorageDownload context fileRef onError onStream = do
    expectAction context (StorageDownload fileRef) $ \case
        Left err -> onError err
        Right (DownloadProcess ftype chunks) -> do
            let fsize = getSum $ foldMap (Sum . fromIntegral . BS.length) chunks
            onStream fsize ftype $ \sink -> do
                mapM_ sink chunks

expectAction :: FakeStorage -> StorageAction a -> (HasCallStack => a -> IO b) -> IO b
expectAction (FakeStorage pBuffer) action cont = do
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
    forall a. AnAction (StorageAction a)

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
