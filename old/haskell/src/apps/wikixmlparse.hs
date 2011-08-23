{-# LANGUAGE DeriveDataTypeable #-}

import System.Console.CmdArgs
import System.Environment
import System.Exit
import Data.Maybe
import Data.List
import Data.DList (DList)
import Debug.Trace
import qualified Data.DList as DList
import Control.Monad
import System.IO
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import Control.Exception

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as LazyByteString
import qualified Codec.Compression.BZip as BZip

import Text.XML.Expat.Proc
import Text.XML.Expat.Tree
import Text.XML.Expat.Format

import WikiMunge.Utils

-- cabal build: cabal install --prefix=/home/alex/Devel/AW/optimal/haskell --user
-- cabal configure for profiling: cabal configure --enable-executable-profiling --ghc-option=-auto-all --ghc-option=-caf-all
-- To rebuild core libraries with profiling enabled:
--   cabal install --reinstall bzlib --enable-library-profiling

-- Run time stats: ./test +RTS --sstderr
-- Profiling: ./test +RTS -p

-- Hexpat for XML parsing, custom parsec (or attoparsec) parser for MediaWiki

--testFile = "../data/Wikipedia-small-snapshot.xml.bz2"
--testFile = "../data/enwikiquote-20110414-pages-articles.xml.bz2"


-- Serialize out to lots of binary files using Data.Binary via BZip.compress


validPage pageData = case pageData of
    (Just _, Just _) -> True
    (_, _) -> False

scanChildren :: [UNode ByteString] -> DList ByteString
scanChildren c = case c of
    h:t -> DList.append (getContent h) (scanChildren t)
    []  -> DList.fromList []

getContent :: UNode ByteString -> DList ByteString
getContent treeElement =
    case treeElement of
        (Element name attributes children)  -> scanChildren children
        (Text text)                         -> DList.fromList [text]

extractText page = do
    revision <- findChild (BS.pack "revision") page
    text <- findChild (BS.pack "text") revision
    return text
    
rawData t = ((getContent.fromJust.fst) t, (getContent.fromJust.snd) t)

-- A list of (title text, page text) pairs
pageDetails :: UNode ByteString -> [(DList ByteString, DList ByteString)]
pageDetails tree =
    let pageNodes = filterChildren relevantChildren tree in
    let getPageData page = (findChild (BS.pack "title") page, extractText page) in
    map rawData $ filter validPage $ map getPageData pageNodes
    where
        relevantChildren node = case node of
            (Element name attributes children) -> name == (BS.pack "page")
            (Text _) -> False

{-
foreachPage pages count = case pages of
    h:t     -> do
        (mapM_ BS.putStr h)
        when ((mod count 1000) == 0) $ hPutStrLn stderr ("Count: " ++ (show count))
        foreachPage t $ count+1
    []      -> return []

outputPages pagesText = do
    let flattenedPages = map DList.toList pagesText
    --mapM_ (mapM_ BS.putStr) flattenedPages
    foreachPage flattenedPages 0
-}


data Options = Options {
    inputFile       :: String,
    outputBase      :: String,
    recordsPerFile  :: Int  
} deriving (Eq, Show, Data, Typeable)

defaultOptions = Options { inputFile = "", outputBase = "", recordsPerFile = 100000 }

splitList :: [a] -> Int -> [[a]]
splitList l splitSize =
    let front = take splitSize l in
    let rest  = drop splitSize l in
    case front of
        []      -> []
        _       -> front : splitList rest splitSize
        
saveRecord :: String -> [(ByteString, ByteString)] -> IO()
saveRecord fileName l = do
    let chunkedList = LazySerializingList l
    let serializable = encode chunkedList
    let compressed = BZip.compress serializable
    print $ "Saving chunk: " ++ fileName
    LazyByteString.writeFile fileName compressed
        
saveRecords :: String -> [[(ByteString, ByteString)]] -> Int -> IO()
saveRecords fileBase lists index =
    case lists of 
        []      -> return ()
        h:t     -> do
            let currentChunkName = fileBase ++ "." ++ (show index) ++ ".bz"
            saveRecord currentChunkName h
            --saveRecords fileBase t (index+1)




main = do
    opts <- cmdArgs defaultOptions
    
    let testFile = inputFile opts
    let outBase = outputBase opts
    let rpf = recordsPerFile opts
    
    let lazyRead fileName = LazyByteString.readFile fileName
    let readCompressed fileName = fmap BZip.decompress $ lazyRead fileName
    let parseXml byteStream = parse defaultParseOptions byteStream :: (UNode ByteString, Maybe XMLParseError)

    rawContent <- readCompressed testFile
    let (tree, mErr) = parseXml rawContent
    let pages = pageDetails tree
    let flattenedPages = map (\x -> ((BS.concat.DList.toList) $ fst x, (BS.concat.DList.toList) $ snd x)) pages
    
    saveRecords outBase (splitList flattenedPages rpf) 0

    putStrLn "Complete!"
    exitWith ExitSuccess


