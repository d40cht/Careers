import System.Exit
import Data.Maybe
import Data.List

import qualified Data.ByteString.Lazy as LazyStr
import qualified Codec.Compression.BZip as BZip

import Text.XML.Expat.Proc
import Text.XML.Expat.Tree
import Text.XML.Expat.Format

-- cabal build: cabal install --prefix=/home/alex/Devel/AW/optimal/haskell --user
-- cabal configure for profiling: cabal configure --enable-executable-profiling

-- Build: ghc -O2 --make -fglasgow-exts test
-- Run time stats: ./test +RTS --sstderr
-- Profiling: ghc -O2 --make -fglasgow-exts -prof -auto-all -caf-all test

-- Hexpat for XML parsing, wikimediaparser for MediaWiki

testFile = "../data/Wikipedia-small-snapshot.xml.bz2"

validPage pageData = case pageData of
    (Just _, Just _) -> True
    (_, _) -> False

rawData t = ((textContent.fromJust.fst) t, (textContent.fromJust.snd) t)

-- The maybe monad!
extractText page = do
    revision <- findChild "revision" page
    text <- findChild "text" revision
    return text

pageDetails tree =
    let pageNodes = filterChildren relevantChildren tree in
    let getPageData page = (findChild "title" page, extractText page) in
    map rawData $ filter validPage $ map getPageData pageNodes
    where
        relevantChildren node = case node of
            (Element name attributes children) -> name == "page"
            (Text _) -> False

main = do
    rawContent <- fmap BZip.decompress (LazyStr.readFile testFile)
    let (tree, mErr) = parse defaultParseOptions rawContent :: (UNode String, Maybe XMLParseError)
    let pages = pageDetails tree
    putStr.(intercalate "\n") $ map snd pages
    putStrLn "Complete!"
    exitWith ExitSuccess


