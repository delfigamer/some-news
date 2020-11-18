module Storage.Fake
    ( ActionExpectation
    , (|>>)
    , FakeStorage
    , checkpoint
    , withFakeStorage
    ) where

import Data.IORef
import GHC.Stack
import Test.Hspec
import Type.Reflection
import HEq1
import Storage

data ActionExpectation
    = forall a. Show a => ActionExpectation
        (Action a)
        (Either StorageError a)
        (forall t u. (HasCallStack => t -> u) -> (t -> u))

instance Show ActionExpectation where
    showsPrec d (ActionExpectation action result _) =
        showParen (d > 0) $
        showsPrec 1 action . showString " |>> " . showsPrec 1 result

infix 1 |>>
(|>>) :: (Show a, HasCallStack) => Action a -> Either StorageError a -> ActionExpectation
action |>> result = ActionExpectation action result id

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
        { doStorage = expectAction context
        }
    checkpoint context []
    return result

expectAction :: FakeStorage -> Action a -> IO (Either StorageError a)
expectAction (FakeStorage pBuffer) action = do
    Context buffer withStackOuter <- readIORef pBuffer
    case buffer of
        ActionExpectation expAction result withStack : rest -> do
            case heq expAction action of
                Just Refl -> do
                    writeIORef pBuffer $ Context rest withStackOuter
                    return result
                Nothing -> do
                    withStack shouldBe (AnAction action) (AnAction expAction)
                    fail "shouldn't reach there"
        [] -> do
            withStackOuter expectationFailure $ "Unexpected action: " ++ show action
            fail "shouldn't reach there"

data AnAction =
    forall a. AnAction (Action a)

instance Show AnAction where
    showsPrec d (AnAction x) = showsPrec d x

instance Eq AnAction where
    _ == _ = False
