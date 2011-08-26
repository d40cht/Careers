import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.HUnit

import Data.Binary
import Data.List

import WikiMunge.Utils
import WikiMunge.Parser

import Text.ParserCombinators.Parsec

test1 = sort [8, 7, 2, 5, 4, 9, 6, 1, 0, 3] @?= [0..9]

test2 =
    let indata = LazySerializingList ["1", "2", "3", "4", "5", "6", "7", "8", "9", "1", "2", "3", "4", "5", "6", "7", "8", "9", "1", "2", "3", "4", "5", "6", "7", "8", "9"] in
    indata @?= (decode.encode) indata
    
templateAndLinkParse =
    let page = "{{ocean habitat topics|image=Callyspongia sp. (Tube sponge).jpg|caption=[[Coral reef]]s provide marine habitats for tube sponges, which in turn become marine habitats for fishes}}" in
    let correctParse = Page [ Template "ocean habitat topics" [ Property "image" [Text "Callyspongia sp. (Tube sponge).jpg"], Property "caption" [InternalLink "Coral reef" [], Text "s provide marine habitats for tube sponges, which in turn become marine habitats for fishes" ] ] ] in
    correctParse @?= runParse page
    
complexTemplate =
    let page = unlines [
            "{{quote box | quote = <center>Shores that look permanent through the short perceptive of a human lifetime are in fact among the most temporary of all marine structures.\n",
            "<ref name=\"Garrison\">Garrison T (2007) [http://books.google.com/books?id=Xvilxa1pz1wC&pg=PA343&dq=%22primary+coasts%22+%22secondary+coasts%22",
            "&hl=en&ei=CqqqTdD7PIimugPHgMmUCg&sa=X&oi=book_result&ct=result&resnum=2&ved=0CCsQ6AEwAQ#v=onepage&q=%22primary%20coasts%22%20%22secondary%20",
            "coasts%22&f=false ''Oceanography: an invitation to marine science''] Cengage Learning, Page 343. ISBN 9780495112860</ref></center>\n",
            "| width = 160px\n",
            "| align = right}}" ] in
    let googleBookLink = "http://books.google.com/books?id=Xvilxa1pz1wC&pg=PA343&dq=%22primary+coasts%22+%22secondary+coasts%22&hl=en&ei=CqqqTdD7PIimugPHgMmUCg&sa=X&oi=book_result&ct=result&resnum=2&ved=0CCsQ6AEwAQ#v=onepage&q=%22primary%20coasts%22%20%22secondary%20coasts%22&f=false" in
    let correctParse = Page [ Template "quote box" [ Property "quote" [Text "<center>Shores that look permanent through the short perceptive of a human lifetime are in fact among the most temporary of all marine structures.\n", Ref "Garrison" [ Text "Garrison T (2007) ", ExternalLink googleBookLink [ Text "''Oceanography: an invitation to marine science''" ], Text " Cengage Learning, Page 343. ISBN 9780495112860" ], Text "</center>\n" ], Property "width" [Text "160px"], Property "align" [Text "right"] ] ] in
    --correctParse @?= runParse page-}
    True @?= True
    
complexLink =
    let page ="[[File:BlueMarble-2001-2002.jpg|thumb|300px|right|Only 29 percent of the world surface is land. The rest is ocean, home to the marine habitats. The oceans average four kilometers in depth and are fringed with coastlines that run for nearly 380,000 kilometres.|alt=Two views of the ocean from space]]" in
    let correctParse = Page [InternalLink "File:BlueMarble-2001-2002.jpg" [Text "Only 29 percent of the world surface is land. The rest is ocean, home to the marine habitats. The oceans average four kilometers in depth and are fringed with coastlines that run for nearly 380,000 kilometres."] ] in
    --correctParse @?= runParse page
    True @?= True

linkWithinALink =
    let page = "[[File:Ocean gravity map.gif|right|thumb|300px|Map of underwater topography (1995 [[NOAA]])]]" in
    let correctParse = Page [InternalLink "File:Ocean gravity map.gif" [Text "Map of underwater topography (1995 ", InternalLink "NOAA" [], Text ")"] ] in
    --correctParse @?= runParse page
    True @?= True


parensParser :: Parser Int
parensParser = do
    let some = do
        char '('
        a <- parensParser
        char ')'
        b <- parensParser
        return (1+a+b)
    some <|> return 0

parsecSimpleTest = do
    let res = parse parensParser "test" "(())(())"
    res @?= (Right 4)
    


tests = [
    testGroup "First group" [
        testCase "Test of test framework" test1,
        testCase "Test of LazySerializingList serialization" test2 ],
    testGroup "Parse tests" [
        testCase "Simple parsec test" parsecSimpleTest,
        testCase "Template and link parse" templateAndLinkParse,
        testCase "Complex multi-line template with ref and center tags" complexTemplate,
        testCase "Complex image link with attributes" complexLink,
        testCase "Link within a link" linkWithinALink ] ]

main = defaultMain tests

