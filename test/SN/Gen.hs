{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SN.Gen
    ( Gen
    , random
    , randomBool
    , randomChar
    , randomWithin
    , suchThat
    , randomPrintableChar
    , randomPrintableString
    , chooseOne
    , chooseFrom
    , chooseAllFrom
    , chooseFromSuch
    , chooseAllFromSuch
    , generate
    ) where

import Control.Monad
import Control.Monad.Reader
import Data.Char
import Data.IORef
import System.IO.Unsafe
import qualified System.Random as R

newtype Gen a = Gen (ReaderT (IORef R.StdGen) IO a)
    deriving (Functor, Applicative, Monad, MonadIO)

runGen :: Gen a -> IORef R.StdGen -> IO a
runGen (Gen (ReaderT f)) = f

gen :: (R.StdGen -> (a, R.StdGen)) -> Gen a
gen f = Gen $ ReaderT $ \state -> atomicModifyIORef' state $ \g1 -> let (x, g2) = f g1 in (g2, x)

random :: R.Uniform a => Gen a
random = gen $ R.uniform

randomBool :: Gen Bool
randomBool = gen R.uniform

randomChar :: Gen Char
randomChar = gen R.uniform

randomWithin :: R.UniformRange a => a -> a -> Gen a
randomWithin x1 x2 = gen $ R.uniformR (x1, x2)

suchThat :: Gen a -> (a -> Bool) -> Gen a
suchThat m test = do
    x <- m
    if test x
        then return x
        else suchThat m test

randomPrintableChar :: Gen Char
randomPrintableChar = do
    cat <- randomWithin 0 99 :: Gen Int
    case () of
        _
            | cat < 70 -> randomWithin '\32' '\126' `suchThat` isPrint
            | cat < 90 -> randomWithin '\128' '\65535' `suchThat` isPrint
            | otherwise -> randomWithin '\65536' maxBound `suchThat` isPrint

randomPrintableString :: Int -> Gen String
randomPrintableString n = replicateM n randomPrintableChar

chooseOne :: [a] -> Gen a
chooseOne [] = error "Gen.chooseOne: empty list"
chooseOne xs = do
    i <- randomWithin 0 (length xs - 1)
    return $ xs !! i

chooseFrom :: Int -> [a] -> Gen [a]
chooseFrom = chooseFromSuch (const True)

chooseAllFrom :: [a] -> Gen [a]
chooseAllFrom = chooseAllFromSuch (const True)

chooseFromSuch :: (a -> Bool) -> Int -> [a] -> Gen [a]
chooseFromSuch filter n = takeSource filter n . permutationSource

chooseAllFromSuch :: (a -> Bool) -> [a] -> Gen [a]
chooseAllFromSuch filter = takeAllSource filter . permutationSource

generate :: Gen a -> IO a
generate m = m `runGen` globalGen

globalGen :: IORef R.StdGen
globalGen = unsafePerformIO $ newIORef =<< R.newStdGen
{-# NOINLINE globalGen #-}

newtype GenSource a = GenSource
    { withGenSource :: forall r. Gen r -> (a -> GenSource a -> Gen r) -> Gen r
    }

takeSource :: (a -> Bool) -> Int -> GenSource a -> Gen [a]
takeSource _ 0 _ = return []
takeSource filter i source = do
    withGenSource source
        (return [])
        (\x next -> if filter x
            then (x :) <$> takeSource filter (i-1) next
            else takeSource filter i next)

takeAllSource :: (a -> Bool) -> GenSource a -> Gen [a]
takeAllSource filter source = do
    withGenSource source
        (return [])
        (\x next -> if filter x
            then (x:) <$> takeAllSource filter next
            else takeAllSource filter next)

permutationSource :: [a] -> GenSource a
permutationSource list0 = do
    let len0 = length list0
    generator len0 list0
  where
    generator len list
        | len <= 0 = GenSource $ \onNull _onCons -> onNull
        | otherwise = GenSource $ \_onNull onCons -> do
            i <- randomWithin 0 (len-1)
            listMinus i list $ \x xs -> do
                onCons x $ generator (len-1) xs
    listMinus _ [] _ = error "empty list"
    listMinus 0 (x : xs) cont = cont x xs
    listMinus i (x : xs) cont = listMinus (i-1) xs $ \y ys -> cont y (x : ys)
