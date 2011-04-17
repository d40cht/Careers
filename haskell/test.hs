import qualified Data.ByteString.Lazy as LazyStr
import qualified Codec.Compression.BZip as BZip

-- Build: ghc --make -fglasgow-exts test

-- HXT for XML parsing, pandoc for MediaWiki, Codec.Compression.BZip for BZip

testFile = "../data/Wikipedia-small-snapshot.xml.bz2"

main :: IO ()
main = do
    content <- fmap BZip.decompress (LazyStr.readFile testFile)
    putStrLn "Complete!"

