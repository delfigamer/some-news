module SN.Data.HList
    ( HList(..)
    , homogenize
    , hfoldr
    ) where

infixr 6 :/
data HList f ts where
    (:/) :: f r -> HList f rs -> HList f (r ': rs)
    E :: HList f '[]

homogenize :: (forall a. f a -> b) -> HList f ts -> [b]
homogenize _ E = []
homogenize f (x :/ xs) = f x : homogenize f xs

hfoldr :: (forall a. f a -> b -> b) -> b -> HList f as -> b
hfoldr _ s E = s
hfoldr f s (x :/ xs) = x `f` hfoldr f s xs
