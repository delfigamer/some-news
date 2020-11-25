{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Binary
    ( BinParser(..)
    , IsBinary(..)
    , fromBinary
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.Int
import Data.Word
import GHC.Generics
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder

newtype BinParser a = BinParser
    { runBinParser
        :: forall r. r -> (a -> BS.ByteString -> r) -> BS.ByteString -> r
    }

instance Functor BinParser where
    fmap f (BinParser p) = BinParser $ \ke kr -> p ke (kr . f)

instance Applicative BinParser where
    pure x = BinParser $ \_ kr -> kr x
    BinParser p <*> BinParser q = BinParser $ \ke kr -> p ke $ \f -> q ke $ \x -> kr (f x)

instance Monad BinParser where
    return = pure
    BinParser p >>= sel = BinParser $ \ke kr -> p ke $ \x -> runBinParser (sel x) ke kr

instance Alternative BinParser where
    empty = BinParser $ \ke _ _ -> ke
    BinParser p <|> BinParser q = BinParser $ \ke kr bs -> p (q ke kr bs) kr bs

instance MonadPlus BinParser where

instance MonadFail BinParser where
    fail _ = empty

class IsBinary a where
    toBinary :: a -> Builder.Builder
    default toBinary :: (Generic a, GIsBinary (Rep a)) => a -> Builder.Builder
    toBinary = gToBinary . from

    parseBinary :: BinParser a
    default parseBinary :: (Generic a, GIsBinary (Rep a)) => BinParser a
    parseBinary = to <$> gParseBinary

fromBinary :: IsBinary a => BS.ByteString -> Maybe a
fromBinary = runBinParser parseBinary Nothing $ \x rest ->
    if BS.null rest
        then Just x
        else Nothing

class GIsBinary a where
    gToBinary :: a p -> Builder.Builder
    gParseBinary :: BinParser (a p)

instance GBinaryElem f => GIsBinary (D1 c f) where
    gToBinary (M1 x) = indexPart <> gToBinaryElem x
      where
        indexPart
            | gConsCount @f <= 1 = mempty
            | gConsCount @f <= 0xff = Builder.word8 $ fromIntegral $ gConsIndex x
            | gConsCount @f <= 0xffff = Builder.word16LE $ fromIntegral $ gConsIndex x
            | otherwise = toBinary $ gConsIndex x
    gParseBinary = do
        ci <- case gConsCount @f of
            cc
                | cc <= 1 -> return 0
                | cc <= 0xff -> fromIntegral <$> parseBinary @Word8
                | cc <= 0xffff -> fromIntegral <$> parseBinary @Word16
                | otherwise -> parseBinary @Word
        guard $ ci < gConsCount @f
        M1 <$> gParseBinaryElem ci

class GBinaryElem a where
    gConsCount :: Word
    gConsIndex :: a p -> Word
    gToBinaryElem :: a p -> Builder.Builder
    gParseBinaryElem :: Word -> BinParser (a p)

instance GBinaryElem V1 where
    gConsCount = 0
    gConsIndex = \case {}
    gToBinaryElem = \case {}
    gParseBinaryElem _ = BinParser $ \ke kr -> \case {}

instance GBinaryElem U1 where
    gConsCount = 1
    gConsIndex _ = 0
    gToBinaryElem U1 = mempty
    gParseBinaryElem _ = return U1

instance IsBinary c => GBinaryElem (Rec0 c) where
    gConsCount = 1
    gConsIndex _ = 0
    gToBinaryElem (K1 x) = toBinary x
    gParseBinaryElem _ = K1 <$> parseBinary

instance GBinaryElem f => GBinaryElem (S1 c f) where
    gConsCount = 1
    gConsIndex _ = 0
    gToBinaryElem (M1 x) = gToBinaryElem x
    gParseBinaryElem ci = M1 <$> gParseBinaryElem ci

instance (GBinaryElem f, GBinaryElem g) => GBinaryElem (f :*: g) where
    gConsCount = 1
    gConsIndex _ = 0
    gToBinaryElem (x :*: y) = gToBinaryElem x <> gToBinaryElem y
    gParseBinaryElem ci = liftA2 (:*:) (gParseBinaryElem ci) (gParseBinaryElem ci)

instance GBinaryElem f => GBinaryElem (C1 c f) where
    gConsCount = 1
    gConsIndex _ = 0
    gToBinaryElem (M1 x) = gToBinaryElem x
    gParseBinaryElem ci = M1 <$> gParseBinaryElem ci

instance (GBinaryElem f, GBinaryElem g) => GBinaryElem (f :+: g) where
    gConsCount = gConsCount @f + gConsCount @g
    gConsIndex = \case
        L1 x -> gConsIndex x
        R1 x -> gConsCount @f + gConsIndex x
    gToBinaryElem = \case
        L1 x -> gToBinaryElem x
        R1 x -> gToBinaryElem x
    gParseBinaryElem ci = if ci < gConsCount @f
        then L1 <$> gParseBinaryElem ci
        else R1 <$> gParseBinaryElem (ci - gConsCount @f)

instance IsBinary Word8 where
    toBinary = Builder.word8
    parseBinary = BinParser $ \ke kr bs -> case BS.uncons bs of
        Just (b, rest) -> kr b rest
        _ -> ke

instance IsBinary Word16 where
    toBinary = Builder.word16LE
    parseBinary = do
        b0 <- parseBinary @Word8
        b1 <- parseBinary @Word8
        return
            $ fromIntegral b0
            + fromIntegral b1 `shiftL` 8

instance IsBinary Word32 where
    toBinary = Builder.word32LE
    parseBinary = do
        b0 <- parseBinary @Word8
        b1 <- parseBinary @Word8
        b2 <- parseBinary @Word8
        b3 <- parseBinary @Word8
        return
            $ fromIntegral b0
            + fromIntegral b1 `shiftL` 8
            + fromIntegral b2 `shiftL` 16
            + fromIntegral b3 `shiftL` 24

instance IsBinary Word64 where
    toBinary = Builder.word64LE
    parseBinary = do
        b0 <- parseBinary @Word8
        b1 <- parseBinary @Word8
        b2 <- parseBinary @Word8
        b3 <- parseBinary @Word8
        b4 <- parseBinary @Word8
        b5 <- parseBinary @Word8
        b6 <- parseBinary @Word8
        b7 <- parseBinary @Word8
        return
            $ fromIntegral b0
            + fromIntegral b1 `shiftL` 8
            + fromIntegral b2 `shiftL` 16
            + fromIntegral b3 `shiftL` 24
            + fromIntegral b4 `shiftL` 32
            + fromIntegral b5 `shiftL` 40
            + fromIntegral b6 `shiftL` 48
            + fromIntegral b7 `shiftL` 56

instance IsBinary Word where
    toBinary = Builder.word64LE . fromIntegral
    parseBinary = fromIntegral <$> parseBinary @Word64

instance IsBinary Int8 where
    toBinary = Builder.int8
    parseBinary = fromIntegral <$> parseBinary @Word8

instance IsBinary Int16 where
    toBinary = Builder.int16LE
    parseBinary = fromIntegral <$> parseBinary @Word16

instance IsBinary Int32 where
    toBinary = Builder.int32LE
    parseBinary = fromIntegral <$> parseBinary @Word32

instance IsBinary Int64 where
    toBinary = Builder.int64LE
    parseBinary = fromIntegral <$> parseBinary @Word64

instance IsBinary Int where
    toBinary = Builder.int64LE . fromIntegral
    parseBinary = fromIntegral <$> parseBinary @Word64

instance IsBinary BS.ByteString where
    toBinary bs = toBinary (BS.length bs) <> Builder.byteString bs
    parseBinary = do
        count <- parseBinary @Int
        BinParser $ \ke kr bs -> do
            let (left, right) = BS.splitAt count bs
            if BS.length left == count
                then kr left right
                else ke

instance IsBinary ()
instance IsBinary Bool
instance IsBinary a => IsBinary [a]
instance IsBinary a => IsBinary (Maybe a)
instance (IsBinary a, IsBinary b) => IsBinary (Either a b)
instance (IsBinary a, IsBinary b) => IsBinary (a, b)
instance (IsBinary a, IsBinary b, IsBinary c) => IsBinary (a, b, c)
instance (IsBinary a, IsBinary b, IsBinary c, IsBinary d) => IsBinary (a, b, c, d)
