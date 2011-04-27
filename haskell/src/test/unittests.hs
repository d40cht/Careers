import Test.HUnit
import Data.Binary

import WikiMunge.Utils

test1 = TestCase( do
    let indata = LazySerializingList ["1", "2", "3", "4", "5", "6", "7", "8", "9", "1", "2", "3", "4", "5", "6", "7", "8", "9", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    assertEqual "Encode followed by decode is equiv to nop" indata $ (decode.encode) indata )

testCases = TestList [test1]

main = do
    runTestTT testCases

