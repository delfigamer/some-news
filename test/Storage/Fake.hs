{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Storage.Fake
    ( UploadExpectation(..)
    , ActionEx(..)
    , ActionExpectation
    , (|>>)
    , FakeStorage
    , checkpoint
    , withFakeStorage
    ) where

import Data.IORef
import GHC.Stack
import Test.Hspec
import Type.Reflection
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import HEq1
import Storage

data UploadExpectation
    = ExpectError StorageError
    | ExpectChunks FileInfo [BS.ByteString] Bool (Maybe StorageError)
    deriving (Show, Eq)

data ActionEx a where
    BaseAction :: Action a -> ActionEx (Either StorageError a)
    UploadAction
        :: Text.Text
        -> Text.Text
        -> Reference Article
        -> Reference User
        -> ActionEx UploadExpectation
    -- DownloadAction
        -- :: Reference FileInfo
        -- -> (StorageError -> IO r)
        -- -> (Int64 -> IO b)
        -- -> (b -> BS.ByteString -> IO b)
        -- -> (b -> IO r)
        -- -> ActionEx ()
deriving instance Show (ActionEx a)
deriving instance Eq (ActionEx a)

instance HEq1 ActionEx where
    heq (BaseAction ba) (BaseAction bb)
        | Just Refl <- heq ba bb = Just Refl
    heq a@(UploadAction {}) b@(UploadAction {})
        | a == b = Just Refl
    heq _ _ = Nothing

data ActionExpectation
    = forall a. Show a => ActionExpectation
        (ActionEx a)
        a
        (forall t u. (HasCallStack => t -> u) -> (t -> u))

instance Show ActionExpectation where
    showsPrec d (ActionExpectation action result _) =
        showParen (d > 0) $
        showsPrec 1 action . showString " |>> " . showsPrec 1 result

infix 1 |>>
class IsActionExpectation a r | a -> r where
    (|>>) :: (Show r, HasCallStack) => a -> r -> ActionExpectation

instance IsActionExpectation (ActionEx a) a where
    action |>> result = ActionExpectation action result id

instance IsActionExpectation (Action a) (Either StorageError a) where
    action |>> result = ActionExpectation (BaseAction action) result id

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
            expectAction context (BaseAction action) return
        , storageUpload = fakeStorageUpload context
        , storageDownload = undefined
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
fakeStorageUpload context name mimeType articleRef userRef uploader0 = do
    expectAction context (UploadAction name mimeType articleRef userRef) $ go uploader0
  where
    go :: HasCallStack => (FileInfo -> IO (Upload r)) -> UploadExpectation -> IO (Either StorageError r)
    go _ (ExpectError err) = return $ Left err
    go uploader (ExpectChunks finfo chunks accept merr) = do
        r <- driveUploader chunks accept $ uploader finfo
        case merr of
            Nothing -> return $ Right r
            Just err -> return $ Left err
    driveUploader :: HasCallStack => [BS.ByteString] -> Bool -> IO (Upload r) -> IO r
    driveUploader [] True func = do
        ur <- func
        AnUpload ur `shouldBe` AnUpload (UploadFinish ())
        let UploadFinish r = ur
        return r
    driveUploader [] False func = do
        ur <- func
        AnUpload ur `shouldBe` AnUpload (UploadAbort ())
        let UploadAbort r = ur
        return r
    driveUploader (x : xs) accept func = do
        ur <- func
        AnUpload ur `shouldBe` AnUpload (UploadChunk x undefined)
        let UploadChunk _ next = ur
        driveUploader xs accept next

expectAction :: FakeStorage -> ActionEx a -> (HasCallStack => a -> IO b) -> IO b
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
    forall a. AnAction (ActionEx a)

instance Show AnAction where
    showsPrec d (AnAction x) = showsPrec d x

instance Eq AnAction where
    _ == _ = False

data AnUpload = forall a. AnUpload (Upload a)

instance Show AnUpload where
    showsPrec d (AnUpload (UploadChunk chunk _)) = showParen (d > 10) $
        showString "UploadChunk " . showsPrec 11 chunk . showString " ..."
    showsPrec d (AnUpload (UploadFinish _)) = showParen (d > 10) $
        showString "UploadFinish ..."
    showsPrec d (AnUpload (UploadAbort _)) = showParen (d > 10) $
        showString "UploadAbort ..."

instance Eq AnUpload where
    (==) (AnUpload (UploadChunk ca _)) (AnUpload (UploadChunk cb _)) = ca == cb
    (==) (AnUpload (UploadFinish _)) (AnUpload (UploadFinish _)) = True
    (==) (AnUpload (UploadAbort _)) (AnUpload (UploadAbort _)) = True
    (==) _ _ = False
