module TData.Relmap
    ( Relmap(..)
    , new
    , insert
    , delete
    , removeLeft
    , removeRight
    ) where

import Control.Monad
import Data.Hashable
import qualified TData.Multimap as TMultimap

data Relmap a b = Relmap
    { left :: TMultimap.Multimap a b
    , right :: TMultimap.Multimap b a
    }

new :: IO (Relmap a b)
new = Relmap <$> TMultimap.new <*> TMultimap.new

insert :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> b -> IO ()
insert (Relmap xtoy ytox) x y = do
    TMultimap.insert xtoy x y
    TMultimap.insert ytox y x

delete :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> b -> IO ()
delete (Relmap xtoy ytox) x y = do
    TMultimap.delete xtoy x y
    TMultimap.delete ytox y x

removeLeft :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> IO ()
removeLeft (Relmap xtoy ytox) x = remove2 xtoy ytox x

removeRight :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> b -> IO ()
removeRight (Relmap xtoy ytox) y = remove2 ytox xtoy y

remove2
    :: (Eq a, Eq b, Hashable a, Hashable b)
    => TMultimap.Multimap a b
    -> TMultimap.Multimap b a
    -> a
    -> IO ()
remove2 xtoy ytox x = do
    ys <- TMultimap.lookupAll xtoy x
    forM_ ys $ \y -> TMultimap.delete ytox y x
    TMultimap.deleteAll xtoy x
