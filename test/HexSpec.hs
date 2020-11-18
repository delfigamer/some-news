module HexSpec
    ( spec
    ) where

import Control.Monad
import Test.Hspec
import Gen
import Hex

spec :: Spec
spec = do
    describe "Hex" $ do
        it "toHex" $ do
            toHex "\x01\x23\x45\x67\x89\xab\xcd\xef" `shouldBe` "0123456789abcdef"
            toHex [0x01, 0x23, 0xcd, 0xef] `shouldBe` "0123cdef"
            toHex [0x01, 0x23, 0xcd, 0xef] `shouldBe` map fromEnum "0123cdef"
            toHex "" `shouldBe` ""
        it "fromHex" $ do
            parseHex "0123456789abcdef" (,) `shouldBe` ("\x01\x23\x45\x67\x89\xab\xcd\xef", "")
            parseHex (map fromEnum "0123cdef") (,) `shouldBe` ("\x01\x23\xcd\xef", [])
            parseHex "0123cdef" (,) `shouldBe` ([0x01, 0x23, 0xcd, 0xef], "")
            parseHex "" (,) `shouldBe` ("", "")
            parseHex "g" (,) `shouldBe` ("", "g")
            parseHex "123" (,) `shouldBe` ("\x12", "3")
            parseHex "123g" (,) `shouldBe` ("\x12", "3g")
        it "parseHex (toHex x) (,) == (x, \"\")" $ do
            forM_ [0..100] $ \i -> do
                x <- generate $ do
                    replicateM i $ randomWithin '\0' '\255'
                parseHex (toHex x) (,) `shouldBe` (x, "")
        it "parseHex x (\\a b -> toHex a ++ b) == x" $ do
            forM_ [0..100] $ \i -> do
                x <- generate $ do
                    replicateM i $ do
                        b <- randomWithin 0 9 :: Gen Int
                        if b == 0
                            then randomWithin '\0' '\255'
                            else head <$> chooseFrom 1 "0123456789abcdef"
                parseHex x $ \a b -> toHex (a :: [Char]) ++ b `shouldBe` x
