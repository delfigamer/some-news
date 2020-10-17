module Gen
    ( Gen
    , randomBool
    , randomChar
    , randomWithin
    , suchThat
    , randomPrintableChar
    , randomPrintableString
    , chooseFrom
    , chooseAllFrom
    , generate
    ) where

import Control.Monad
import Data.Char
import Data.IORef
import System.IO.Unsafe
import qualified System.Random as R

newtype Gen a = Gen { runGen :: forall r. R.StdGen -> (R.StdGen -> a -> r) -> r }

instance Functor Gen where
    fmap = liftM

instance Applicative Gen where
    pure = return
    (<*>) = ap

instance Monad Gen where
    return x = Gen $ \g cont -> cont g x
    m >>= f = Gen $ \g1 cont -> runGen m g1 $ \g2 x -> runGen (f x) g2 cont

gen :: (R.StdGen -> (a, R.StdGen)) -> Gen a
gen f = Gen $ \g1 cont -> let (x, g2) = f g1 in cont g2 x

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

chooseFrom :: [a] -> Int -> Gen [a]
chooseFrom list n = takeSource n $ permutationSource list

chooseAllFrom :: [a] -> Gen [a]
chooseAllFrom list = takeAllSource $ permutationSource list

generate :: Gen a -> IO a
generate m = atomicModifyIORef' globalGen $ \g1 -> runGen m g1 $ \g2 x -> (g2, x)

globalGen :: IORef R.StdGen
globalGen = unsafePerformIO $ newIORef =<< R.newStdGen
{-# NOINLINE globalGen #-}

newtype GenSource a = GenSource (Gen (Maybe (a, GenSource a)))

takeSource :: Int -> GenSource a -> Gen [a]
takeSource 0 _ = return []
takeSource i (GenSource f) = do
    iter <- f
    case iter of
        Just (x, next) -> (x:) <$> takeSource (i-1) next
        Nothing -> return []

takeAllSource :: GenSource a -> Gen [a]
takeAllSource (GenSource f) = do
    iter <- f
    case iter of
        Just (x, next) -> (x:) <$> takeAllSource next
        Nothing -> return []

permutationSource :: [a] -> GenSource a
permutationSource list0 = do
    let len0 = length list0
    GenSource $ sourceFunc len0 list0
  where
    sourceFunc 0 _ = return Nothing
    sourceFunc len list = do
        i <- randomWithin 0 (len-1)
        let (x, xs) = listMinus i list
        return $ Just (x, GenSource $ sourceFunc (len-1) xs)
    listMinus _ [] = error "empty list"
    listMinus 0 (x:xs) = (x, xs)
    listMinus i (x:xs) = (x:) <$> listMinus (i-1) xs
