{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE EmptyDataDeriving #-}

module BinarySpec
    ( spec
    ) where

import Control.Monad
import Data.Int
import Data.Word
import GHC.Generics
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as LBS
import Binary
import Gen
import Logger

data Stuff
    = ConI Int8 Int16 Int32 Int64 Int
    | ConW Word8 Word16 Word32 Word64 Word
    | ConT [Int] BS.ByteString (BS.ByteString, (), Maybe Word)
    | ConU
    | ConMV (Either MyVoid ())
    deriving (Show, Eq, Generic, IsBinary)

data MyVoid
    deriving (Show, Eq, Generic, IsBinary)

randomStuff :: Gen Stuff
randomStuff = do
    con <- randomWithin 0 4
    case con :: Int of
        0 -> ConI <$> random <*> random <*> random <*> random <*> random
        1 -> ConW <$> random <*> random <*> random <*> random <*> random
        2 -> ConT <$> randomList random <*> randomBS <*>
            ((,,) <$> randomBS <*> pure () <*> randomMaybe random)
        3 -> pure ConU
        4 -> pure $ ConMV $ Right ()
  where
    randomList g = do
        l <- randomWithin 0 32
        replicateM l g
    randomBS = BS.pack <$> randomList random
    randomMaybe g = do
        b <- randomBool
        case b of
            False -> pure Nothing
            True -> Just <$> g
    randomEither g h = do
        b <- randomBool
        case b of
            False -> Left <$> g
            True -> Right <$> h

spec :: Spec
spec = do
    describe "Binary" $
        it "converts data" $ do
            replicateM_ 10000 $ do
                withTestLogger $ \logger -> do
                    a <- generate $ randomStuff
                    let lbs = Builder.toLazyByteString $ toBinary a
                    logDebug logger $ "Source: " <<| a
                    logDebug logger $ "Serial: " <<| LBS.unpack lbs
                    fromBinary (LBS.toStrict lbs) `shouldBe` Just a
