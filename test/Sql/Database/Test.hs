{-# LANGUAGE StandaloneDeriving #-}

module Sql.Database.Test
    ( Action(..)
    , ActionExpectation
    , (|>>)
    , withTestDatabase
    ) where

import Data.IORef
import qualified Data.Text as Text
import Sql.Query
import qualified Sql.Database as Db
import Test.Hspec
import Tuple
import Unsafe.Coerce

data Action a where
    Query :: Query result -> Action (Maybe result)
    BeginTransaction :: Action ()
    CommitTransaction :: Action ()
deriving instance Show (Action a)

instance HEq1 Action where
    Query a ~= Query b = a ~= b
    BeginTransaction ~= BeginTransaction = True
    CommitTransaction ~= CommitTransaction = True
    _ ~= _ = False

data ActionExpectation where
    ActionExpectation
        :: Action a
        -> a
        -> (forall t u. (HasCallStack => t -> u) -> (t -> u))
        -> ActionExpectation

instance Show ActionExpectation where
    showsPrec d (ActionExpectation action _ _) = showParen (d > 0) $
        showsPrec 1 action . showString " |>> ..."

infixr 0 |>>
(|>>) :: HasCallStack => Action a -> a -> ActionExpectation
action |>> result = ActionExpectation action result id

data AnAction =
    forall a. AnAction (Action a)

instance Show AnAction where
    showsPrec d (AnAction x) = showsPrec d x

instance Eq AnAction where
    AnAction a == AnAction b = a ~= b

withTestDatabase
    :: (([ActionExpectation] -> IO ()) -> Db.Handle -> IO r)
    -> IO r
withTestDatabase body = do
    pexpectations <- newIORef $ []
    result <- body (actionCheckpoint pexpectations) $
        Db.Handle
            { Db.query = testDbQuery pexpectations
            , Db.withTransaction = testDbWithTransaction pexpectations
            }
    actionCheckpoint pexpectations []
    return result

testDbQuery :: IORef [ActionExpectation] -> Query result -> IO (Maybe result)
testDbQuery pexpectations query = do
    expectAction pexpectations $ Query query

testDbWithTransaction :: IORef [ActionExpectation] -> IO r -> IO r
testDbWithTransaction pexpectations body = do
    expectAction pexpectations $ BeginTransaction
    result <- body
    expectAction pexpectations $ CommitTransaction
    return result

expectAction :: IORef [ActionExpectation] -> Action a -> IO a
expectAction pexpectations action = do
    expectations <- readIORef pexpectations
    case expectations of
        top@(ActionExpectation expaction result withStack):rest -> do
            withStack shouldBe (AnAction action) (AnAction expaction)
            writeIORef pexpectations $! rest
            return $ unsafeCoerce result -- type equality was asserted by `shouldBe` earlier
        [] -> do
            expectationFailure $ "Unexpected action: " ++ show action
            undefined

actionCheckpoint :: IORef [ActionExpectation] -> [ActionExpectation] -> IO ()
actionCheckpoint pexpectations nextbatch = do
    expectations <- readIORef pexpectations
    case expectations of
        ActionExpectation _ _ withStack:_ -> withStack expectationFailure $ "Missing actions: " ++ show expectations
        [] -> return ()
    writeIORef pexpectations nextbatch
