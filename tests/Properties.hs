{-# LANGUAGE FlexibleInstances, TypeSynonymInstances #-}

module Main (main) where

import Data.HashRing (HashRing, (!))
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
    , testProperty "replicas is # of replicas" pReplicas
    , testProperty "equality" pInequality ]
  , testGroup "query properties"
    [ testProperty "lookup in empty ring" pLookupEmpty
    , testProperty "member identity" pMember
    , testProperty "lookup identity" pLookupId
    , testProperty "find identity" pFindId
    , testProperty "index (bang) identity" pIndexId
    , testProperty "lookup not Nothing" pLookupJust
    , testProperty "lookup wraps" pLookupWrap ]
  , testGroup "insert/delete properties"
    [ testProperty "insert into empty" pSingleton
    , testProperty "insert idempotency" pInsertIdem
    , testProperty "insert then delete" pInsertDelete
    , testProperty "delete nonmember" pDeleteNonMember ]
  , testGroup "conversion properties"
    [ testProperty "show/read identity" pShowRead
    , testProperty "list conversion" pListConvert ] ]


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

pInequality :: NReps -> Int -> Bool
pInequality (NReps nreps) node = R.singleton nreps node /= R.empty nreps


pLookupEmpty :: Int -> NReps -> Bool
pLookupEmpty node (NReps nreps) =
  R.lookup node (R.empty nreps :: IRing) == Nothing

pMember :: Int -> IRing -> Bool
pMember node ring = R.member node (R.insert node ring) == True

pLookupId :: Int -> IRing -> Bool
pLookupId msgnode ring = R.lookup msgnode (R.insert msgnode ring) == Just msgnode

pFindId :: Int -> IRing -> Bool
pFindId msgnode ring = R.find msgnode (R.insert msgnode ring) == msgnode

pIndexId :: Int -> IRing -> Bool
pIndexId msgnode ring = R.insert msgnode ring ! msgnode == msgnode

pLookupJust :: Int -> IRing -> Property
pLookupJust msg ring = not (R.null ring) ==> R.lookup msg ring /= Nothing

pLookupWrap :: Int -> NReps -> Property
pLookupWrap msgnode (NReps nreps) = msgnode /= maxBound ==>
  R.lookup (msgnode + 1) (R.insert msgnode $ R.empty nreps) == Just msgnode


pSingleton :: NReps -> Int -> Bool
pSingleton (NReps nreps) node =
  R.insert node (R.empty nreps) == R.singleton nreps node

pInsertIdem :: Int -> IRing -> Bool
pInsertIdem node ring =
  R.insert node (R.insert node ring) == R.insert node ring

pInsertDelete :: Int -> IRing -> Bool
pInsertDelete node ring
    | (R.member node ring) = True -- if item is in ring - it's removing will break the ring
    | otherwise = ring == R.delete node (R.insert node ring)

pDeleteNonMember :: Int -> IRing -> Property
pDeleteNonMember node ring =
  not (R.member node ring) ==> R.delete node ring == ring


pShowRead :: IRing -> Bool
pShowRead ring = ring == read (show ring)

pListConvert :: NReps -> [Int] -> Bool
pListConvert (NReps nreps) ints =
  let innodes = nub ints
      outnodes = R.toList $ R.fromList nreps innodes
  in innodes \\ outnodes == [] && outnodes \\ innodes == []
