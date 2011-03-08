import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import scala.collection.immutable.HashSet

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}
 
class TestSuite extends FunSuite
{

    private def parseSurfaceForm( raw : String ) : String =
    {
        val markupFiltered = raw.filter( _ != '\'' )
        
        return markupFiltered
    }


    test("A first test")
    {
        println( "Running a test" )
        assert( 3 === 5-2 )
        assert( "a" === "a" )
        
        val testStr = "ABC"
        intercept[StringIndexOutOfBoundsException]
        {
            testStr(-1)
        }
    }
    
    test("A simple dbpedia test")
    {
        val topicTitle = "Hello_World"
        val topicText = "[[Blah|'''blah blah''']] ''An italicised '''bit''' of text'' <b>Some markup</b>"
        
        println( topicText.filter(_ != '\'' ) )

        val markupParser = WikiParser()
        val page = new WikiPage( WikiTitle.parse( topicTitle.toString ), 0, 0, topicText.toString )
        val parsed = markupParser( page )
        
        println( parsed.toString() )
    }
    
    
    test("A simple phrasemap test")
    {
        val pm = new PhraseMap()
        
        pm.addPhrase( "hello" )
        pm.addPhrase( "hello world" )
        pm.addPhrase( "hell" )
        pm.addPhrase( "hello world, la de la" )
        
        val testPhrase = "hello birds, hello sky, hello world. it's a hell of a day to be saying hello. hello world, la de la de la."
        
        type PhraseListType = List[(List[Char], PhraseNode)]
    
        def phrasesUpdate( el : Char, activeList : PhraseListType, phraseRegFn : String => Unit ) : PhraseListType =
        {
            activeList match
            {
                case (headChars, headPhraseNode)::tail =>
                {
                    if ( headPhraseNode.isTerminal )
                    {
                        phraseRegFn( "Phrase: " + headChars.reverse.mkString("") )
                    }
                    
                    headPhraseNode.walk(el) match
                    {
                        case Some( rest ) =>
                        {
                            return (el::headChars, rest)::phrasesUpdate( el, tail, phraseRegFn )
                        }
                        case None =>
                        {
                            return phrasesUpdate( el, tail, phraseRegFn )
                        }
                    }
                }
                case Nil =>
                {
                    return Nil
                }
            }
        }
        
        var lastChar = ' '
        var activePhrases : PhraseListType = Nil
        for (c <- testPhrase )
        {
            c match
            {
                case ' ' =>
                {
                }
                case other @ _ =>
                {
                    if ( lastChar == ' ' )
                    {
                        activePhrases = (List[Char](), pm.root)::activePhrases
                    }
                    activePhrases = phrasesUpdate( c, activePhrases, println )
                }
            }
            lastChar = c
        }
    }
}
