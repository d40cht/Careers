module Text.WikimediaParser where

import Text.ParserCombinators.Parsec
import Data.List (intercalate)

data WikiMarkup = Text String | Link {text::String, target::String} deriving (Eq)

instance Show WikiMarkup where
  show (Text t) = t
  show (Link t s) = "link '" ++ t ++ "' -> '" ++ s ++ "'"

data Page = Page [WikiMarkup] deriving (Eq)

instance Show Page where
  show (Page xs) = foldl (\x y -> x ++ (show y)) "" xs

generalParseLink p = between (string "[[") (string "]]") p

isNot c = satisfy (/= c)

betweenMatching :: Char -> GenParser Char st [Char]
betweenMatching c = do open <- many1 $ char c 
                       content <- (many $ isNot c)
                       count (length open) (char c)
                       return content

parseLink :: Parser WikiMarkup
parseLink = generalParseLink linkContent
            where linkContent = do target <- optionMaybe (try $ manyTill anyToken (char '|'))
                                   text <- many1 (noneOf "[]") 
                                   return $ Link text (maybe text id target)

parseNoWiki :: Parser WikiMarkup
parseNoWiki = between (string "<nowiki>") (string "</nowiki>") parseText

parseCategory :: Parser WikiMarkup
parseCategory = generalParseLink linkContent
                where linkContent = do option "" (string ":")
                                       string "Catégorie"
                                       many (noneOf "]")
                                       return $ Text ""

eol = char '\n' 
notEol = noneOf "\r\n"

parseUnorderedListItem = do char '*'
                            many notEol
                            eol

parseUnorderedList :: Parser WikiMarkup
parseUnorderedList = do many1 parseUnorderedListItem
                        return $ Text "" 

parseOrderedListItem = do char '#'
                          many notEol 
                          eol

parseOrderedList :: Parser WikiMarkup
parseOrderedList = do many1 parseUnorderedListItem
                      return $ Text ""

parseLinkNameSpace :: Parser WikiMarkup
parseLinkNameSpace = generalParseLink linkContent
                     where linkContent = do manyTill anyToken (char ':')
                                            text <- many (noneOf "[]")
                                            return $ Text text

parseLinkOtherLanguage :: Parser WikiMarkup
parseLinkOtherLanguage = generalParseLink linkContent
                         where linkContent = do anyToken
                                                anyToken
                                                option "" (string "mple")
                                                char ':'
                                                many (noneOf "[]")
                                                return $ Text "" 

parseUnnamedLink :: Parser WikiMarkup
parseUnnamedLink = do between (char '[') (char ']') (many $ noneOf "]")
                      return $ Text ""

parseHTMLComment :: Parser WikiMarkup
parseHTMLComment = do string "<!--"
                      manyTill (string "-->") anyToken
                      return $ Text ""

{-
parseHTML = do char '<'
               tagName <- many $ noneOf " >"
               options <- option "" $ manyTill anyToken (char '>')
               content <- nest
               string "</"
               endTag <- string tagName
               string ">"
               return $ intercalate "," $ [tagName, options, content, endTag] 
            where nest = do text <- many $ isNot '<'
                            case text of
                              "" -> do return ""
                              _ -> do
                                html <- option "" parseHTML
                                case html of
                                  "" -> return text
                                  _ -> do
                                    next <- nest
                                    case next of
                                      "" -> return $ text ++ html
                                      _ -> return $ text ++ html ++ next
-}

parseHeading :: Parser WikiMarkup
parseHeading = do try $ betweenMatching '=' 
                  return $ Text ""

parseBrackets :: Parser WikiMarkup
parseBrackets = do between (string "{{") (string "}}") (many (noneOf "}"))
                   return $ Text ""

symbols :: String
symbols = "[{=*<"

symbol :: Parser Char
symbol = oneOf symbols

symbolW :: Parser WikiMarkup
symbolW = symbol >>= (return . Text . (\a->[a]))

notSymbol = noneOf symbols

parseSymbols = try parseCategory <|>
               try parseLink <|>
               try parseUnnamedLink <|>
               try parseBrackets <|>
               try parseHeading <|>
               try parseUnorderedList <|>
               try parseOrderedList <|>
               try parseHTMLComment <|>
               symbolW

parseText :: Parser WikiMarkup
parseText = do text <- many1 notSymbol
               return $ Text text

parseLine :: Parser WikiMarkup
parseLine = parseSymbols <|>
            parseText

parseArticle :: Parser [WikiMarkup]
parseArticle = do vals <- many parseLine
                  eof
                  return vals

runWikiParse :: String -> [WikiMarkup]
runWikiParse input = do 
    case (parse parseArticle "" input) of 
        Left a  -> []
        Right b -> b

concatText :: [WikiMarkup] -> [WikiMarkup]
concatText (Text x: Text y:xs) = Text (x ++ y) : concatText xs
concatText [] = []
concatText (x:xs) = x : concatText xs
