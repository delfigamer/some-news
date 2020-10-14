{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Tuple
    ( HList(..)
    , TypeList(..)
    , All(..)
    , AllWith(..)
    , constrainHListWrapped
    -- , HEq1(..)
    , homogenize
    , ListCat
    , (++/)
    , takeHList
    , traverseHListGiven
    -- , ListJoin
    -- , joinHList
    -- , reshapeHList
    -- , zipHList
    ) where

import Data.Kind
import Data.Proxy
import Data.Typeable

infixr 6 :/
data HList f (ts :: [k]) where
    (:/) :: f r -> HList f rs -> HList f (r ': rs)
    E :: HList f '[]

class TypeList ts where
    proxyHList :: proxy ts -> HList Proxy ts

instance TypeList '[] where
    proxyHList _ = E

instance TypeList ts => TypeList (t ': ts) where
    proxyHList _ = Proxy :/ proxyHList Proxy

class TypeList ts => All c ts where
    constrainHList
        :: cproxy c
        -> HList g ts
        -> (ts ~ '[] => r)
        -> (forall u us. (ts ~ (u ': us), c u, All c us) => g u -> HList g us -> r)
        -> r

instance All c '[] where
    constrainHList _ E onNull _ = onNull

instance (c t, All c ts) => All c (t ': ts) where
    constrainHList _ (x :/ xs) _ onCons = onCons x xs

class TypeList ts => AllWith f c ts where
    constrainHListWith
        :: fproxy f
        -> cproxy c
        -> HList g ts
        -> (ts ~ '[] => r)
        -> (forall u us. (ts ~ (u ': us), c (f u), AllWith f c us) => g u -> HList g us -> r)
        -> r

instance AllWith f c '[] where
    constrainHListWith _ _ E onNull _ = onNull

instance (c (f t), AllWith f c ts) => AllWith f c (t ': ts) where
    constrainHListWith _ _ (x :/ xs) _ onCons = onCons x xs

constrainHListWrapped
    :: forall g c ts cproxy r. AllWith g c ts
    => cproxy c
    -> HList g ts
    -> (ts ~ '[] => r)
    -> (forall u us. (ts ~ (u ': us), c (g u), AllWith g c us) => g u -> HList g us -> r)
    -> r
constrainHListWrapped = constrainHListWith (Proxy :: Proxy g)

instance forall f ts. AllWith f Show ts => Show (HList f ts) where
    showsPrec d tup = constrainHListWrapped (Proxy :: Proxy Show) tup
        (showString "E")
        (\x xs -> showParen (d > 6) $ showsPrec 7 x . showString " :* " . showsPrec 7 xs)

-- infix 4 ~=
-- class HEq1 (f :: k -> *) where
    -- (~=) :: f a -> f b -> Bool

-- instance HEq1 f => HEq1 (TupleT f) where
    -- E ~= E = True
    -- (x :* xs) ~= (y :* ys) = x ~= y && xs ~= ys
    -- _ ~= _ = False

homogenize :: (forall a. f a -> b) -> HList f ts -> [b]
homogenize _ E = []
homogenize f (x :/ xs) = f x:homogenize f xs

type family ListCat ts us where
    ListCat '[] us = us
    ListCat (t ': ts) us = t ': ListCat ts us

infixr 6 ++/
(++/) :: HList f ts -> HList f us -> HList f (ListCat ts us)
E ++/ tup = tup
(x :/ xs) ++/ tup = x :/ (xs ++/ tup)

takeHList :: HList f ts -> HList g (ListCat ts us) -> (HList g ts -> HList g us -> r) -> r
takeHList E ys cont = cont E ys
takeHList (_ :/ xs) (y :/ ys) cont = takeHList xs ys $ \mine others -> cont (y :/ mine) others

traverseHListGiven :: (All c ts, Applicative m) => proxy c -> HList f ts -> (forall a. c a => f a -> m (g a)) -> m (HList g ts)
traverseHListGiven cproxy tup f = constrainHList cproxy tup
    (pure E)
    (\x xs -> (:/) <$> f x <*> traverseHListGiven cproxy xs f)

-- type family ListJoin tss where
    -- ListJoin '[] = '[]
    -- ListJoin (ts ': uss) = ListCat ts (ListJoin uss)

-- joinHList :: HList (HList f) tss -> HList f (ListJoin tss)
-- joinHList E = E
-- joinHList (xs :/ xss) = xs ++/ joinHList xss

-- reshapeHList :: HList (HList f) tss -> HList g (ListJoin tss) -> HList (HList g) tss
-- reshapeHList E E = E
-- reshapeHList (xs :/ xss) y = takeHList xs y $ \ys yss -> ys :/ reshapeHList xss yss

-- zipHList :: (forall a. f a -> g a -> h a) -> HList f ts -> HList g ts -> HList h ts
-- zipHList _ E E = E
-- zipHList f (x :/ xs) (y :/ ys) = f x y :/ zipHList f xs ys
