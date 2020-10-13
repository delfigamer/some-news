{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies #-}

module Tuple
    ( TupleT(..)
    , HEq1(..)
    , foldrTuple
    , mapTuple
    , ListCat
    , (>*)
    , splitTuple
    ) where

import Data.Functor.Identity
import Data.Proxy
import Data.Typeable

infixr 6 :*
data TupleT f (ts :: [*]) where
    (:*) :: f t -> TupleT f rs -> TupleT f (t ': rs)
    E :: TupleT f '[]

instance (forall a. Show (f a)) => Show (TupleT f ts) where
    showsPrec _ E = showString "E"
    showsPrec d (x :* xs) =
        showParen (d > 6) $
        showsPrec 7 x . showString " :* " . showsPrec 7 xs

infix 4 ~=
class HEq1 (f :: k -> *) where
    (~=) :: f a -> f b -> Bool

instance HEq1 f => HEq1 (TupleT f) where
    E ~= E = True
    (x :* xs) ~= (y :* ys) = x ~= y && xs ~= ys
    _ ~= _ = False

foldrTuple :: (forall a. f a -> b -> b) -> b -> TupleT f ts -> b
foldrTuple _ seed E = seed
foldrTuple f seed (x :* xs) = f x $ foldrTuple f seed xs

mapTuple :: (forall a. f a -> b) -> TupleT f ts -> [b]
mapTuple f = foldrTuple (\x acc -> f x:acc) []

type family ListCat (ts :: [*]) (us :: [*]) :: [*] where
    ListCat '[] us = us
    ListCat (t ': ts) us = t ': ListCat ts us

infixr 6 >*
(>*) :: TupleT f ts -> TupleT f us -> TupleT f (ListCat ts us)
E >* tup = tup
(x :* xs) >* tup = x :* (xs >* tup)

splitTuple :: TupleT f ts -> TupleT g (ListCat ts us) -> (TupleT g ts -> TupleT g us -> r) -> r
splitTuple E ys cont = cont E ys
splitTuple (_ :* xs) (y :* ys) cont = splitTuple xs ys $ \mine others -> cont (y :* mine) others
