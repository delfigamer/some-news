module SN.Data.Relmap
    ( Relmap(..)
    , new
    , insert
    , delete
    , removeLeft
    , removeRight
    ) where

import Control.Monad
import Data.Hashable
import qualified SN.Data.Multimap as Multimap

data Relmap a b = Relmap
    { left :: Multimap.Multimap a b
    , right :: Multimap.Multimap b a
    }

new :: IO (Relmap a b)
new = Relmap <$> Multimap.new <*> Multimap.new

insert :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> b -> IO ()
insert (Relmap xtoy ytox) x y = do
    Multimap.insert xtoy x y
    Multimap.insert ytox y x

delete :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> b -> IO ()
delete (Relmap xtoy ytox) x y = do
    Multimap.delete xtoy x y
    Multimap.delete ytox y x

removeLeft :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> a -> IO ()
removeLeft (Relmap xtoy ytox) x = remove2 xtoy ytox x

removeRight :: (Eq a, Eq b, Hashable a, Hashable b) => Relmap a b -> b -> IO ()
removeRight (Relmap xtoy ytox) y = remove2 ytox xtoy y

remove2
    :: (Eq a, Eq b, Hashable a, Hashable b)
    => Multimap.Multimap a b
    -> Multimap.Multimap b a
    -> a
    -> IO ()
remove2 xtoy ytox x = do
    ys <- Multimap.lookupAll xtoy x
    forM_ ys $ \y -> Multimap.delete ytox y x
    Multimap.deleteAll xtoy x
