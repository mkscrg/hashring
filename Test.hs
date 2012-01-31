module Test
where


import qualified Crypto.Hash.MD5 as MD5
import Data.Bits
import qualified Data.ByteString as B
import Data.Hashable
import Data.Word


newtype Node = Node B.ByteString

instance Hashable Node where
    hashWithSalt i (Node bs) = bytesToBits
                             $ MD5.hash
                             $ bs `B.append` intToBytes i

intToBytes :: Int -> B.ByteString
intToBytes x = B.reverse $ B.unfoldr go (bitSize x `div` 8, x)
  where
    go (0, _) = Nothing
    go (nbytes, x') = Just ( fromIntegral x' .&. (maxBound :: Word8)
                           , (nbytes - 1, x' `shiftR` 8) )

bytesToBits :: Bits a => B.ByteString -> a
bytesToBits bs =
    let zero = 0
    in B.foldl (\x w -> x `shiftL` 8 .|. fromIntegral w) zero $
       B.take (bitSize zero `div` 8) bs
