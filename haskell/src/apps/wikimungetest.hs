{-# LANGUAGE DeriveDataTypeable #-}

import System.Console.CmdArgs
import Data.Binary

import qualified Data.ByteString.Lazy as LazyByteString
import qualified Codec.Compression.BZip as BZip
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS


import WikiMunge.Utils

data Options = Options {
    inputFiles      :: [String]
} deriving (Eq, Show, Data, Typeable)

defaultOptions = Options { inputFiles = [] }

dumpTitles :: String -> IO ()
dumpTitles fileName = do
    print fileName
    raw <- fmap BZip.decompress (LazyByteString.readFile fileName)
    let decoded = decode raw :: (LazySerializingList (ByteString, ByteString))
    case decoded of
        (LazySerializingList l)     -> mapM_ (putStrLn.BS.unpack.fst) l
        _                           -> return ()
        

main = do
    opts <- cmdArgs defaultOptions
    mapM_ dumpTitles $ inputFiles opts
