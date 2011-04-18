import System.Exit
import Data.Maybe
import Data.List
import Data.DList (DList)
import qualified Data.DList as DList

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
testFile = "../data/enwiki-latest-pages-articles.xml.bz2"

validPage pageData = case pageData of
    (Just _, Just _) -> True
    (_, _) -> False
    
--getContent treeElement = textContent treeElement

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

--testValue :: Char8
--testValue = BS.pack "Boo!"

-- The maybe monad!
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

main = do
    rawContent <- fmap BZip.decompress (LazyByteString.readFile testFile)
    -- Perhaps the UNode String bit below should be UNode LazyStr
    let (tree, mErr) = parse defaultParseOptions rawContent :: (UNode ByteString, Maybe XMLParseError)
    --let (tree, mErr) = parse defaultParseOptions rawContent :: (UNode String, Maybe XMLParseError)
    let pages = pageDetails tree
    --BS.putStr.(BS.intercalate (BS.pack "\n")) $ map snd pages
    let blah = map snd pages
    let flattenedPages = map DList.toList blah
    mapM_ (mapM_ BS.putStr) flattenedPages
    putStrLn "Complete!"
    exitWith ExitSuccess


