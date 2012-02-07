{-# LANGUAGE FlexibleInstances, TypeSynonymInstances #-}

module Main (main) where

import Data.HashRing (HashRing)
import qualified Data.HashRing as R
import Data.List ((\\), nub)
import Data.Word (Word8)
import Test.QuickCheck (Arbitrary (..), Gen, Property, (==>), listOf, resize)
import Test.Framework (defaultMain, testGroup )
import Test.Framework.Providers.QuickCheck2 (testProperty)


main :: IO ()
main = defaultMain
  [ testGroup "basic properties"
    [ testProperty "empty ring is null" pEmptyIsNull
    , testProperty "nonempty rings are not null" pNonemptyNonNull
    , testProperty "size is # of nodes" pSize
    , testProperty "replicas is # of replicas" pReplicas ]
  , testGroup "query properties"
    [ testProperty "member identity" pMember
    , testProperty "lookup identity" pLookup
    , testProperty "find identity" pFind ]
  , testGroup "insert/delete properties"
    [ testProperty "insert into empty" pSingleton
    , testProperty "insert idempotency" pInsertIdem
    , testProperty "insert then delete" pInsertDelete
    , testProperty "delete nonmember" pDeleteNonMember ]
  , testProperty "list conversion" pListConvert ]


type IRing = HashRing Int
newtype NReps = NReps Int deriving (Show)

instance Arbitrary NReps where
  arbitrary = fmap (NReps . (+1) . fromIntegral) (arbitrary :: Gen Word8)

instance Arbitrary IRing where
  arbitrary = do
    NReps nreps <- arbitrary
    nodes <- resize 100 $ listOf arbitrary
    return $ R.fromList nreps nodes


pEmptyIsNull :: NReps -> Bool
pEmptyIsNull (NReps nreps) = R.null $ R.empty nreps

pNonemptyNonNull :: IRing -> Property
pNonemptyNonNull ring = R.size ring > 0 ==> not (R.null ring)

pSize :: NReps -> [Int] -> Bool
pSize (NReps nreps) nodes = go nodes $ R.empty nreps
  where
    go [] ring = R.size ring == length (nub nodes)
    go (n:ns) ring = go ns $ R.insert n ring

pReplicas :: NReps -> Bool
pReplicas (NReps nreps) = R.replicas (R.empty nreps) == nreps

pSingleton :: NReps -> Int -> Bool
pSingleton (NReps nreps) node =
  R.insert node (R.empty nreps) == R.singleton nreps node

pMember :: Int -> IRing -> Bool
pMember node ring = R.member node (R.insert node ring) == True

pLookup :: Int -> IRing -> Bool
pLookup node ring = R.lookup node (R.insert node ring) == Just node

pFind :: Int -> IRing -> Bool
pFind node ring = R.find node (R.insert node ring) == node

pInsertIdem :: Int -> IRing -> Bool
pInsertIdem node ring =
  R.insert node (R.insert node ring) == R.insert node ring

pInsertDelete :: Int -> IRing -> Bool
pInsertDelete node ring = ring == R.delete node (R.insert node ring)

pDeleteNonMember :: Int -> IRing -> Property
pDeleteNonMember node ring =
  not (R.member node ring) ==> R.delete node ring == ring

pListConvert :: NReps -> [Int] -> Bool
pListConvert (NReps nreps) ints =
  let innodes = nub ints
      outnodes = R.toList $ R.fromList nreps innodes
  in innodes \\ outnodes == [] && outnodes \\ innodes == []
