import System.Exit
import Data.Maybe

import qualified Data.ByteString.Lazy as LazyStr
import qualified Codec.Compression.BZip as BZip

import Text.XML.Expat.Proc
import Text.XML.Expat.Tree
import Text.XML.Expat.Format

-- Build: ghc --make -fglasgow-exts test

-- HXT for XML parsing, wikimediaparser for MediaWiki

-- putStrLn (show (1 + 1)) == putStrLn $ show (1 + 1) = putStrLn $ show $ 1 + 1
-- ($ makes everything on the right higher precedence than on the left. Used for avoiding brackets).

testFile = "../data/Wikipedia-small-snapshot.xml.bz2"


pageDetails tree =
    let pageNodes = filterChildren relevantChildren tree in
    pageNodes
    where
        relevantChildren node = case node of
            (Element name attributes children) -> name == "page"
            (Text _) -> False


main :: IO ()
main = do
    rawContent <- fmap BZip.decompress (LazyStr.readFile testFile)
    let (tree, mErr) = parse defaultParseOptions rawContent :: (UNode String, Maybe XMLParseError)
    let pages = pageDetails tree
    --let pages = filterChildren relevantChildren tree
    -- <page> <title>Boo</title> <text>Some wikipedia wisdom</text>
    --let relevantChildren node cheitherildren tag text = tag == "
    -- pages <- filterChildren \
    let titles = mapMaybe (findChild "title") pages
    print titles
    putStrLn "Complete!"
    exitWith ExitSuccess

