module TData.Tree
    ( Tree
    , new
    , fromList
    , validate
    , include
    , exclude
    , adjust
    , trySetParent
    , keys
    , toList
    , withoutSubtreesOf
    , lookup
    , lookup'
    , member
    , childrenList
    , subtreeList
    ) where

import Prelude hiding (foldr, lookup)
import Control.Concurrent.STM
import Control.Concurrent.STM.TVar
import Control.Monad
import Data.Hashable
import Data.Maybe
import GHC.Stack
import qualified DeferredFolds.UnfoldlM as UnfoldlM
import qualified StmContainers.Map as StmMap
import qualified StmContainers.Set as StmSet

data Tree a b = Tree
    { treeCells :: StmMap.Map a (TreeCell a b)
    , treeRoots :: StmMap.Map a (TreeCell a b)
    }

data TreeCell a b = TreeCell
    { cellData :: TVar b
    , cellParent :: TVar (a, Maybe (TreeCell a b))
    , cellChildren :: StmMap.Map a (TreeCell a b)
    }

instance Eq (TreeCell a b) where
    (==) x y = (==) (cellData x) (cellData y)

new :: IO (Tree a b)
new = Tree <$> StmMap.newIO <*> StmMap.newIO

fromList :: (Eq a, Hashable a) => [(a, b, a)] -> IO (Tree a b)
fromList xs = do
    tree <- new
    forM_ xs $ \(k, v, pk) -> include tree k v pk
    return tree

validate :: (Eq a, Hashable a) => Tree a b -> IO ()
validate (Tree cells roots) = atomically $ do
    UnfoldlM.forM_ (StmMap.unfoldlM cells) $ \(k, cell) -> do
        (pk, mParent) <- readTVar (cellParent cell)
        mParent2 <- StmMap.lookup pk cells
        assert $ mParent2 == mParent
        mCell2 <- case mParent of
            Nothing -> StmMap.lookup k roots
            Just parent -> StmMap.lookup k (cellChildren parent)
        assert $ mCell2 == Just cell
        UnfoldlM.forM_ (StmMap.unfoldlM (cellChildren cell)) $ \(ck, child) -> do
            (k3, mCell3) <- readTVar (cellParent child)
            assert $ k3 == k
            assert $ mCell3 == Just cell
    UnfoldlM.forM_ (StmMap.unfoldlM roots) $ \(k, cell) -> do
        (pk, mParent) <- readTVar (cellParent cell)
        assert $ mParent == Nothing
        mParent2 <- StmMap.lookup pk cells
        assert $ mParent2 == Nothing
        checkLoops cell []
  where
    checkLoops cell ancestry = do
        assert $ cell `notElem` ancestry
        UnfoldlM.forM_ (StmMap.unfoldlM (cellChildren cell)) $ \(_, child) -> do
            checkLoops child (cell : ancestry)
    assert :: HasCallStack => Bool -> STM ()
    assert b = if b
        then return ()
        else error "TData.Tree.validate: invalid tree"

include :: (Eq a, Hashable a) => Tree a b -> a -> b -> a -> IO ()
include (Tree cells roots) k v pk = atomically $ do
    mparent <- StmMap.lookup pk cells
    cell <- TreeCell
        <$> newTVar v
        <*> newTVar (pk, mparent)
        <*> StmMap.new
    StmMap.insert cell k cells
    case mparent of
        Nothing -> StmMap.insert cell k roots
        Just parent -> StmMap.insert cell k (cellChildren parent)

exclude :: (Eq a, Hashable a) => Tree a b -> a -> IO (Maybe a)
exclude (Tree cells roots) k = atomically $ do
    mCell <- StmMap.lookup k cells
    case mCell of
        Nothing -> return Nothing
        Just cell -> do
            (pk, mParent) <- readTVar (cellParent cell)
            StmMap.delete k cells
            case mParent of
                Nothing -> StmMap.delete k roots
                Just parent -> StmMap.delete k (cellChildren parent)
            UnfoldlM.forM_ (StmMap.unfoldlM (cellChildren cell)) $ \(ck, child) -> do
                case mParent of
                    Nothing -> StmMap.insert child ck roots
                    Just parent -> StmMap.insert child ck (cellChildren parent)
                writeTVar (cellParent child) (pk, mParent)
            return $ Just pk

adjust :: (Eq a, Hashable a) => Tree a b -> a -> (b -> b) -> IO ()
adjust (Tree cells _) k f = atomically $ do
    mcell <- StmMap.lookup k cells
    case mcell of
        Nothing -> return ()
        Just cell -> modifyTVar' (cellData cell) f

trySetParent :: (Eq a, Hashable a) => Tree a b -> a -> a -> IO Bool
trySetParent (Tree cells roots) k newPk = atomically $ do
    mCell <- StmMap.lookup k cells
    case mCell of
        Nothing -> return True
        Just cell -> do
            (oldPk, mOldParent) <- readTVar (cellParent cell)
            if newPk == oldPk
                then return True
                else do
                    mNewParent <- StmMap.lookup newPk cells
                    loops <- doesLoop (newPk, mNewParent)
                    if loops
                        then return False
                        else do
                            case mOldParent of
                                Nothing -> StmMap.delete k roots
                                Just oldParent -> StmMap.delete k (cellChildren oldParent)
                            case mNewParent of
                                Nothing -> StmMap.insert cell k roots
                                Just newParent -> StmMap.insert cell k (cellChildren newParent)
                            writeTVar (cellParent cell) (newPk, mNewParent)
                            return True
  where
    doesLoop (pk, mParent) = do
        if pk == k
            then return True
            else case mParent of
                Nothing -> return False
                Just parent -> do
                    grand <- readTVar (cellParent parent)
                    doesLoop grand

keys :: Tree a b -> IO [a]
keys (Tree cells _) = atomically $ do
    UnfoldlM.foldlM' (\buf (k, _) -> return $ k : buf) [] $
        StmMap.unfoldlM cells

toList :: Tree a b -> IO [(a, b, a)]
toList (Tree cells roots) = atomically $ do
    UnfoldlM.foldlM' iter [] $
        StmMap.unfoldlM cells
  where
    iter buf (k, cell) = do
        v <- readTVar $ cellData cell
        (pk, _) <- readTVar $ cellParent cell
        return $ (k, v, pk) : buf

withoutSubtreesOf :: (Eq a, Hashable a) => Tree a b -> [a] -> IO [(a, b, a)]
withoutSubtreesOf (Tree _ roots) minusList = do
    minusSet <- StmSet.newIO
    forM_ minusList $ \mk -> do
        atomically $ StmSet.insert mk minusSet
    atomically $ collect minusSet [] roots
  where
    collect minusSet buf submap = do
        UnfoldlM.foldlM' (iter minusSet) buf $ StmMap.unfoldlM submap
    iter minusSet buf (k, cell) = do
        isExcluded <- StmSet.lookup k minusSet
        if isExcluded
            then return buf
            else do
                v <- readTVar (cellData cell)
                (pk, _) <- readTVar (cellParent cell)
                collect minusSet ((k, v, pk) : buf) (cellChildren cell)

lookup :: (Eq a, Hashable a) => Tree a b -> a -> IO (Maybe (a, b, a))
lookup (Tree cells _) k = atomically $ do
    mCell <- StmMap.lookup k cells
    case mCell of
        Nothing -> return Nothing
        Just cell -> do
            v <- readTVar (cellData cell)
            (pk, _) <- readTVar (cellParent cell)
            return $ Just (k, v, pk)

lookup' :: (Eq a, Hashable a) => Tree a b -> a -> IO (a, b, a)
lookup' tree k = do
    mr <- lookup tree k
    case mr of
        Just r -> return r
        Nothing -> error "TData.Tree.lookup': no value"

member :: (Eq a, Hashable a) => Tree a b -> a -> IO Bool
member tree k = isJust <$> lookup tree k

childrenList :: (Eq a, Hashable a) => Tree a b -> a -> IO [(a, b, a)]
childrenList (Tree cells _) k = atomically $ do
    mCell <- StmMap.lookup k cells
    case mCell of
        Nothing -> return []
        Just cell -> do
            UnfoldlM.foldlM' iter [] $ StmMap.unfoldlM (cellChildren cell)
  where
    iter buf (k, cell) = do
        v <- readTVar (cellData cell)
        (pk, _) <- readTVar (cellParent cell)
        return $ (k, v, pk) : buf

subtreeList :: (Eq a, Hashable a) => Tree a b -> a -> IO [(a, b, a)]
subtreeList (Tree cells _) k = atomically $ do
    mCell <- StmMap.lookup k cells
    case mCell of
        Nothing -> return []
        Just cell -> iter [] (k, cell)
  where
    collect buf submap = do
        UnfoldlM.foldlM' iter buf $ StmMap.unfoldlM submap
    iter buf (k, cell) = do
        v <- readTVar (cellData cell)
        (pk, _) <- readTVar (cellParent cell)
        collect ((k, v, pk) : buf) (cellChildren cell)
