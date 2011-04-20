import System.Exit
import Data.Maybe
import Data.List
import Data.DList (DList)
import Debug.Trace
import qualified Data.DList as DList
import Control.Monad
import System.IO

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as LazyByteString
import qualified Codec.Compression.BZip as BZip

import Text.XML.Expat.Proc
import Text.XML.Expat.Tree
import Text.XML.Expat.Format

-- cabal build: cabal install --prefix=/home/alex/Devel/AW/optimal/haskell --user
-- cabal configure for profiling: cabal configure --enable-executable-profiling --ghc-option=-auto-all --ghc-option=-caf-all
-- To rebuild core libraries with profiling enabled:
--   cabal install --reinstall bzlib --enable-library-profiling

-- Run time stats: ./test +RTS --sstderr
-- Profiling: ./test +RTS -p

-- Hexpat for XML parsing, custom parsec (or attoparsec) parser for MediaWiki

--testFile = "../data/Wikipedia-small-snapshot.xml.bz2"
--testFile = "../data/enwikiquote-20110414-pages-articles.xml.bz2"

testFile = "../data/enwiki-latest-pages-articles.xml.bz2"

-- Serialize out to lots of binary files using Data.Binary via BZip.compress

-- PROFILING: Check just bzip, then just title parsing

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

rawData t = ((getContent.fromJust.fst) t, (getContent.fromJust.snd) t)

extractText page = do
    revision <- findChild (BS.pack "revision") page
    text <- findChild (BS.pack "text") revision
    return text

pageDetails tree =
    let pageNodes = filterChildren relevantChildren tree in
    let getPageData page = (findChild (BS.pack "title") page, extractText page) in
    map rawData $ filter validPage $ map getPageData pageNodes
    where
        relevantChildren node = case node of
            (Element name attributes children) -> name == (BS.pack "page")
            (Text _) -> False
            
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

lazyRead fileName = LazyByteString.readFile fileName
readCompressed fileName = fmap BZip.decompress $ lazyRead fileName
parseXml byteStream = parse defaultParseOptions byteStream :: (UNode ByteString, Maybe XMLParseError)

main = do
    rawContent <- readCompressed testFile
    let (tree, mErr) = parseXml rawContent
    let pages = pageDetails tree
    let pagesText = map snd pages
    outputPages pagesText
    putStrLn "Complete!"
    exitWith ExitSuccess


