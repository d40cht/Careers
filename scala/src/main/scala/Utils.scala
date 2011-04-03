import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import java.io.StringReader
import org.dbpedia.extraction.wikiparser._

import scala.util.matching.Regex

object Utils
{
    def normalize( raw : String ) : String =
    {
        // Deal with: double/triple spaces, embedded xml tags
        return raw.toLowerCase().filter(_ != '\'' )
    }
    
    def luceneTextTokenizer( page : String ) : List[String] =
    {
    	val textSource = new StringReader( page )
        val tokenizer = new StandardTokenizer( LUCENE_30, textSource )
    
        var run = true
        var wordList : List[String] = Nil
        while ( run )
        {
            val nextTerm = tokenizer.getAttribute(classOf[TermAttribute]).term()
            if ( nextTerm != "" )
            {
                wordList = nextTerm :: wordList
            }
            run = tokenizer.incrementToken()
        }
        tokenizer.close()
        return wordList.reverse
    }
    

    def traverseWikiTree( element : Node, visitorFn : Node => Unit )
    {
        visitorFn( element )
        element match
        {
            case InternalLinkNode(destination, children, line) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case PageNode( title, id, revision, isRedirect, isDisambig, children ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case SectionNode( name, level, children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case TemplateNode( title, children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case TableNode( caption, children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case TableRowNode( children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case TableCellNode( children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case PropertyNode( value, children, line ) => for ( child <- children ) traverseWikiTree( child, visitorFn )
            case TextNode( text, line ) =>
            
            case _ =>
        }
    }
    
    class LinkExtractor
    {
        var inFirstSection = true
        val linkRegex = new Regex( "[=]+[^=]+[=]+" )
        
        private def visitNode( element : Node, contextAddFn : (String, String, String, Boolean) => Unit )
        {
            element match
            {
                case InternalLinkNode( destination, children, line ) =>
                {
                    if ( children.length != 0 &&
                        (destination.namespace.toString() == "Main" ||
                         destination.namespace.toString() == "Category") )
                    {
                        val destinationTopic = destination.decoded
                        
                        val first = children(0)
                        first match
                        {
                            case TextNode( surfaceForm, line ) =>
                            {
                                val normalizedText = normalize( surfaceForm )

                                // TODO: Annotate if in first paragraph. If redirect only take first
                                contextAddFn( normalizedText, destination.namespace.toString, destination.decoded.toString, inFirstSection )
                            }
                            case _ =>
                            {
                            }
                        }
                    }
                }
                case TextNode( text, line ) =>
                {
                    // Nothing for now
                    linkRegex.findFirstIn(text) match
                    {
                        case None =>
                        case _ => inFirstSection = false
                    }
                }
            }
        }
        
        def run( element : Node, contextAddFn : (String, String, String, Boolean) => Unit )
        {
            traverseWikiTree( element, node => visitNode(node, contextAddFn) )
        }
    }
   
    
    class LinkExtractorOld
    {
        var inFirstSection = true
        val linkRegex = new Regex( "[=]+[^=]+[=]+" )
        
        def extractLinks( element : Node, contextAddFn : (String, String, String, Boolean) => Unit )
        {
            element match
            {
                case InternalLinkNode(destination, children, line) =>
                {
                    
                    // TODO: Allow Main and Category
                    if ( children.length != 0 &&
                        (destination.namespace.toString() == "Main" ||
                         destination.namespace.toString() == "Category") )
                    {
                        val destinationTopic = destination.decoded
                        
                        val first = children(0)
                        first match
                        {
                            case TextNode( surfaceForm, line ) =>
                            {
                                val normalizedText = normalize( surfaceForm )

                                // TODO: Annotate if in first paragraph. If redirect only take first
                                contextAddFn( normalizedText, destination.namespace.toString, destination.decoded.toString, inFirstSection )
                            }
                            case _ =>
                            {
                            }
                        }
                    }
                }
                
                case TextNode( text, line ) =>
                {
                    // Nothing for now
                    linkRegex.findFirstIn(text) match
                    {
                        case None =>
                        case _ => inFirstSection = false
                    }
                }

                case PageNode( title, id, revision, isRedirect, isDisambig, children ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case SectionNode( name, level, children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case TemplateNode( title, children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case TableNode( caption, children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case TableRowNode( children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case TableCellNode( children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )
                case PropertyNode( value, children, line ) => for ( child <- children ) extractLinks( child, contextAddFn )

                case _ =>
            }
        }
    }
}
