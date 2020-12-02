module Hex
    ( toHex
    , hex
    , parseHex
    ) where

import Data.String

toHex :: (Enum e1, Enum e2) => [e1] -> [e2]
toHex [] = []
toHex (c:cs) = hchar (x `div` 16) : hchar (x `mod` 16) : toHex cs
  where
    x = fromEnum c
    hchar n
        | n < 10 = toEnum $ n + 48
        | otherwise = toEnum $ n + 87

hex :: IsString a => String -> a
hex = fromString . toHex

parseHex
    :: (Enum e1, Enum e2)
    => [e1]
    -> ([e2] -> [e1] -> r)
    -> r
parseHex as@(a:b:cs) cont = unhexOne a b
    (cont [] as)
    (\i -> parseHex cs $ \is -> cont (i : is))
parseHex cs cont = cont [] cs

unhexOne :: (Enum e1, Enum e2) => e1 -> e1 -> r -> (e2 -> r) -> r
unhexOne a b f t =
    hord (fromEnum a) f $ \ai ->
        hord (fromEnum b) f $ \bi ->
            t $ toEnum $ ai * 16 + bi
  where
    hord x f t
        | 48 <= x && x <= 57 = t $ x - 48
        | 97 <= x && x <= 102 = t $ x - 87
        | otherwise = f
