module WikiMunge.Parser where

type WikiStringType = String
type WikiURL = String
type ExternalURL = String

data WikiElement =
    Page [WikiElement]                          |
    Text WikiStringType                         |
    Comment                                     |
    Ref WikiStringType [WikiElement]            |
    Math                                        |
    Code                                        |
    Source                                      |
    InternalLink WikiURL [WikiElement]          |
    ExternalLink ExternalURL [WikiElement]      |
    Property WikiStringType [WikiElement]       |
    Template WikiURL [WikiElement]              |
    Section WikiStringType                      |
    TableCell WikiStringType                    |
    TableRow [WikiElement]                      |
    Table [WikiElement]
    deriving (Eq, Show)
    


runParse :: WikiStringType -> WikiElement
runParse inputString =
    Page [ Template "ocean habitat topics" [
        Property "image" [Text "Callyspongia sp. (Tube sponge).jpg"],
        Property "caption" [InternalLink "Coral reef" [], Text "s provide marine habitats for tube sponges, which in turn become marine habitats for fishes" ] ] ]
