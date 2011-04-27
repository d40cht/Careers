{-# LANGUAGE DeriveDataTypeable #-}

import System.Console.CmdArgs
import Data.Binary

import qualified Data.ByteString.Lazy as LazyByteString
import qualified Codec.Compression.BZip as BZip
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS


import WikiMunge.Utils
import Text.WikimediaParser

data Options = Options {
    inputFiles      :: [String]
} deriving (Eq, Show, Data, Typeable)

defaultOptions = Options { inputFiles = [] }

parsePage :: (ByteString, ByteString) -> IO ()
parsePage pageData = do
    let title = (BS.unpack.fst) pageData
    let article = (BS.unpack.snd) pageData
    let els = runWikiParse article
    putStrLn title
    mapM_ (putStrLn.show) els
    --putStrLn els

dumpTitles :: String -> IO ()
dumpTitles fileName = do
    print fileName
    raw <- fmap BZip.decompress (LazyByteString.readFile fileName)
    let decoded = decode raw :: (LazySerializingList (ByteString, ByteString))
    case decoded of
        (LazySerializingList l)     -> mapM_ parsePage l
        _                           -> return ()
        

main = do
    opts <- cmdArgs defaultOptions
    mapM_ dumpTitles $ inputFiles opts
