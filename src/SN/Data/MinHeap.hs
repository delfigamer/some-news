module SN.Data.MinHeap
    ( MinHeap
    , empty
    , insert
    , uncons
    , break
    , Arg(..)
    ) where

import Prelude hiding (break, head, length, tail, uncons)
import Data.Foldable
import Data.Semigroup

{- Pairing heap -}

data MinHeap a
    = Empty
    | MinHeap !(Tree a)
    deriving (Show)

instance Ord a => Semigroup (MinHeap a) where
    Empty <> a = a
    a <> Empty = a
    MinHeap ta <> MinHeap tb = MinHeap (meld ta tb)

instance Ord a => Monoid (MinHeap a) where
    mempty = Empty

infixr 4 :-
data TreeList a
    = E
    | (:-) !(Tree a) !(TreeList a)

instance Show a => Show (TreeList a) where
    showsPrec _ ts = showList (conv ts)
      where
        conv E = []
        conv (a :- as) = a : conv as

data Tree a
    = Tree !a !(TreeList a)
    deriving (Show)

mergePairs :: Ord a => TreeList a -> MinHeap a
mergePairs E = Empty
mergePairs (t :- E) = MinHeap t
mergePairs (ta :- tb :- rest) = MinHeap (meld ta tb) <> mergePairs rest

meld :: Ord a => Tree a -> Tree a -> Tree a
meld a@(Tree ah ats) b@(Tree bh bts)
    | ah < bh = Tree ah (b :- ats)
    | otherwise = Tree bh (a :- bts)

empty :: MinHeap a
empty = Empty

insert :: Ord a => a -> MinHeap a -> MinHeap a
insert x Empty = MinHeap (Tree x E)
insert x (MinHeap (Tree h ts))
    | x < h = MinHeap (Tree x (Tree h E :- ts))
    | otherwise = MinHeap (Tree h (Tree x E :- ts))

uncons :: Ord a => MinHeap a -> Maybe (a, MinHeap a)
uncons Empty = Nothing
uncons (MinHeap (Tree h ts)) = Just (h, mergePairs ts)

break :: Ord a => (a -> Bool) -> MinHeap a -> ([a], MinHeap a)
break cond heap = go id heap
  where
    go buf a = case a of
        MinHeap (Tree h ts)
            | cond h ->
                go (\xs -> buf (h : xs)) (mergePairs ts)
        _ -> (buf [], a)
