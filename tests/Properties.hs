module Main (main) where

import qualified Data.HashRing as R
import Test.QuickCheck
import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)


main :: IO ()
main = defaultMain tests

tests :: [Test]
tests =
  [ testProperty "empty ring is null" pEmptyIsNull
  , testProperty "empty ring is empty" pEmptyIsEmpty ]


type IntRing = R.HashRing Int

instance Arbitrary a => Arbitrary (HashRing a) where
  arbitrary = 

pEmptyIsNull :: Int -> Bool
pEmptyIsNull nreps = R.null $ R.empty nreps

pEmptyIsEmpty :: Int -> Bool
pEmptyIsEmpty nreps = R.size (R.empty nreps) == 0
