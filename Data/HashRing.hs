-- | module: Data.HashRing
-- copyright: (c) Michael S. Craig 2012
-- license: BSD3
-- maintainer: mkscrg@gmail.com
-- stability: experimental
-- portability: portable
--
-- An efficient implementation of consistent hashing, as described in
--
--    * David Karger et al., \"/Consistent hashing and random trees:
--    distributed caching protocols for relieving hot spots on the World Wide
--    Web/\", 29th Annual ACM Symposium on Theory,
--    <http://dl.acm.org/citation.cfm?id=258660>
--
-- In distributed computing applications, it's usually necessary route messages
-- to some group of N nodes in the network. Message locality, wherein messages
-- of the same kind are routed to the same node, is often desirable. \"Normal\"
-- hashing, where a message's node is determined by some hash function modulo
-- N, has the undesirable property that adding or removing a node from the
-- network causes the key sets of all other nodes to change drastically.
-- In contrast, consistent hashing has the property that small changes to the
-- size of the node set cause only small changes to key sets of the nodes.
--
-- This implementation is built on top of 'IntMap' and 'Set'. It provides
-- /O(1)/ lookup functions as well as /O(min(log n, R))/ insertion and deletion
-- functions, where /R/ is the number of replica nodes used in the ring (see
-- 'empty').
--
-- The key space of the ring is the full range of 'Int' values. To insert a
-- node, we generate (/R > 0/) keys by hashing the node with /R/ successive
-- salts, and the node is referenced by those keys in the ring. To get a node
-- for a message, we hash the message to an 'Int' value /k/ and find the
-- smallest key /k'/ in the ring such that /k <= k'/. The node is the value
-- referenced by /k'/. Higher values of /R/ give a more even distribution of
-- keys to nodes but slow down insertions and deletions of nodes. /R/ is
-- specified when constructing a 'HashRing' with 'empty', 'singleton', or
-- 'fromList' and retrievable with 'replicas'.
--
-- The ability of 'HashRing' to fairly distribute messages among nodes relies
-- on the implementations of 'Data.Hashable.hashWithSalt' for the message and
-- node types. For example, the default implementation for
-- 'Data.ByteString.ByteString' is non-uniform on short inputs, and so it's
-- unsuitable for use with 'HashRing'. Reimplementing
-- 'Data.Hashable.hashWithSalt' for your message and node types with a
-- cryptographic hash function (like MD5 or SHA1 from the @cryptohash@ package)
-- will give better results.

module Data.HashRing (
  -- * Map type
  HashRing
  -- * Construction
, empty
, singleton
  -- * Operators
, (!)
  -- * Query
, null
, size
, replicas
, member
, lookup
, find
  -- * Insertion
, insert
  -- * Deletion
, delete
  -- * Conversion
, fromList
, toList
) where

import Prelude hiding (lookup, null)
import Data.Hashable (Hashable, hash, hashWithSalt)
import Data.IntMap (IntMap)
import qualified Data.IntMap as IM
import Data.Set (Set)
import qualified Data.Set as S


-- | The constructor for this data type is not exported. See 'empty',
-- 'singleton', or 'fromList'.
--
-- Note that 'HashRing' is parameterized by the node type and not by the
-- message type. As made clear by the type signatures for '!', 'find', and
-- 'lookup', any 'Hashable' type can be used as a message.
data HashRing a = HashRing
  { nodeMap :: IntMap a
  , nodeSet :: Set a
  , replicas :: Int
  -- ^ Number of replica nodes (/R/) in the ring for each real node.
  }


instance Show a => Show (HashRing a) where
  showsPrec d ring = showParen (d > 10)
                   $ showString "fromList "
                   . shows (replicas ring)
                   . showString " "
                   . shows (toList ring)

instance (Read a, Ord a, Hashable a) => Read (HashRing a) where
  readsPrec d = readParen (d > 10) $ \s -> do
    ("fromList", s') <- lex s
    (nreps, s'') <- reads s'
    (nodes, s''') <- reads s''
    return (fromList nreps nodes, s''')


-- | Construct an empty ring with a specific /R/ value.
empty :: Int -> HashRing a
empty nreps = HashRing IM.empty S.empty nreps

-- | Construct a single-node ring with a specific /R/ value.
singleton :: (Ord a, Hashable a) => Int -> a -> HashRing a
singleton nreps node = insert node $ empty nreps


-- | @True@ if the ring is empty, @False@ otherwise.
null :: HashRing a -> Bool
null ring = S.null $ nodeSet ring

-- | Number of nodes in the ring.
size :: HashRing a -> Int
size ring = S.size $ nodeSet ring

-- | @True@ if the node is in the ring, @False@ otherwise.
member :: Ord a => a -> HashRing a -> Bool
member node ring = S.member node $ nodeSet ring


-- | Get the node in the ring corresponding to a message, or @Nothing@ if the
-- ring is empty.
lookup :: Hashable b => b -> HashRing a -> Maybe a
lookup msg ring = let nmap = nodeMap ring in if IM.null nmap
  then Nothing
  else Just $ case IM.splitLookup (hash msg) nmap of
    (_, Just node, _) -> node
    (_, _, submap) -> if IM.null submap
      then snd $ IM.findMin nmap
      else snd $ IM.findMin submap

-- | Get the node in the ring corresponding to a message, or error if the ring
-- is empty.
find :: Hashable b => b -> HashRing a -> a
find msg ring = case lookup msg ring of
  Just x -> x
  Nothing -> error "HashRing.find: empty hash ring"

-- | Get the node in the ring corresponding to a message, or error if the ring
-- is empty.
(!) :: Hashable b => b -> HashRing a -> a
msg ! ring = find msg ring


-- | Add a node to the ring.
insert :: (Ord a, Hashable a) => a -> HashRing a -> HashRing a
insert node ring
  | S.member node $ nodeSet ring = ring
  | otherwise = ring
    { nodeMap = foldr (\key nmap -> IM.insert key node nmap) (nodeMap ring)
              $ take (replicas ring)
              $ filter (\key -> not $ IM.member key (nodeMap ring))
              $ nodeKeys node
    , nodeSet = S.insert node (nodeSet ring) }

-- | Remove a node from the ring.
delete :: (Ord a, Hashable a) => a -> HashRing a -> HashRing a
delete node ring
  | not $ S.member node $ nodeSet ring = ring
  | otherwise = ring
    { nodeMap = foldr (\key nmap -> IM.delete key nmap) (nodeMap ring)
              $ take (replicas ring)
              $ filter (\key -> IM.lookup key (nodeMap ring) == Just node)
              $ nodeKeys node
    , nodeSet = S.delete node (nodeSet ring) }


-- | Construct a ring from an /R/ value and a list of nodes.
fromList :: (Ord a, Hashable a) => Int -> [a] -> HashRing a
fromList nreps nodes = foldr insert (empty nreps) nodes

-- | Construct a list containing the nodes in the ring.
toList :: HashRing a -> [a]
toList ring = S.toList $ nodeSet ring


-- List of possible keys for a new node
nodeKeys :: Hashable a => a -> [Int]
nodeKeys node = map (\i -> hashWithSalt i node) [0..]
