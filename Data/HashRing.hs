module Data.HashRing
( HashRing
, empty
, fromList
, insert
, delete
, lookup
) where

import Prelude hiding (lookup)
import Data.Hashable (Hashable, hash, hashWithSalt)
import Data.IntMap (IntMap)
import qualified Data.IntMap as IM
import Data.Set (Set)
import qualified Data.Set as S


data HashRing a = HashRing
                { nodeMap :: IntMap a
                , nodeSet :: Set a
                , nReplicas :: Int }

empty :: Int -> HashRing a
empty nreps = HashRing IM.empty S.empty nreps

fromList :: (Ord a, Hashable a) => [a] -> Int -> HashRing a
fromList nodes nreps = foldr insert (empty nreps) nodes

insert :: (Ord a, Hashable a) => a -> HashRing a -> HashRing a
insert node ring = ring
    { nodeMap = foldr (\key nmap -> IM.insert key node nmap) (nodeMap ring)
              $ take (nReplicas ring)
              $ filter (\key -> not $ IM.member key (nodeMap ring))
              $ nodeKeys node
    , nodeSet = S.insert node (nodeSet ring) }

delete :: (Ord a, Hashable a) => a -> HashRing a -> HashRing a
delete node ring = if not $ S.member node (nodeSet ring) then ring else ring
    { nodeMap = foldr (\key nmap -> IM.delete key nmap) (nodeMap ring)
              $ take (nReplicas ring)
              $ filter (\key -> IM.lookup key (nodeMap ring) == Just node)
              $ nodeKeys node
    , nodeSet = S.delete node (nodeSet ring) }

lookup :: Hashable b => b -> HashRing a -> Maybe a
lookup msg ring = if IM.null (nodeMap ring)
    then Nothing
    else Just $ case IM.splitLookup (hash msg) (nodeMap ring) of
        (_, Just node, _) -> node
        (_, _, submap) -> snd $ IM.findMin submap


nodeKeys :: Hashable a => a -> [Int]
nodeKeys node = map (\i -> hashWithSalt i node) [0..]
