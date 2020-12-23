{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE UndecidableInstances #-}

module SN.Data.PolyMap
    ( PolyMap
    , empty
    , lookup
    , insert
    ) where

import Prelude hiding (lookup)
import Data.Hashable
import Type.Reflection
import Unsafe.Coerce
import qualified Data.HashMap.Strict as Map

{- polymorphic map, where type of the value is held in the key -}
{- values are kept unevaluated -}
newtype PolyMap f = PolyMap (Map.HashMap (PKey f) PValue)

data PKey f = forall a. PKey !(TypeRep a) !(f a)
data PValue = forall a. PValue a

type EqPoly f = forall b. Eq (f b)
type HashablePoly f = forall b. Hashable (f b)

instance EqPoly f => Eq (PKey f) where
    PKey tr1 v1 == PKey tr2 v2 = case eqTypeRep tr1 tr2 of
        Just HRefl -> v1 == v2
        Nothing -> False

instance HashablePoly f => Hashable (PKey f) where
    s `hashWithSalt` PKey tr v = s `hashWithSalt` tr `hashWithSalt` v

empty :: PolyMap f
empty = PolyMap Map.empty

lookup :: (EqPoly f, HashablePoly f, Typeable a) => f a -> PolyMap f -> Maybe a
lookup key (PolyMap inner) = case Map.lookup (PKey typeRep key) inner of
    Just (PValue x) -> Just (unsafeCoerce x)
    Nothing -> Nothing

insert :: (EqPoly f, HashablePoly f, Typeable a) => f a -> a -> PolyMap f -> PolyMap f
insert key value (PolyMap inner) = PolyMap (Map.insert (PKey typeRep key) (PValue value) inner)
