package org.seacourt.utility

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import java.io.{StringReader, OutputStream}
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage

import scala.util.matching.Regex
import org.apache.hadoop.io.{Writable}
import java.io.{DataInput, DataOutput, FileInputStream, FileOutputStream, File}

import java.io.{DataOutput, DataOutputStream, DataInput, DataInputStream, ByteArrayInputStream}
import java.util.ArrayList

import scala.collection.mutable.{IndexedSeqOptimized, Builder, ArrayLike, LinkedList}


trait FixedLengthSerializable
{
    def size : Int
    def loadImpl( in : DataInput )
    def saveImpl( out : DataOutput )
    
    final def save( out : DataOutputStream )
    {
        val before = out.size()
        saveImpl( out )
        val padding = size - (out.size() - before)
        assert( padding >= 0 )
        for ( i <- 0 until padding ) out.writeByte(0)
    }
    
    final def load( in : DataInput )
    {
        //val before = in.size()
        loadImpl( in )
        //val after = out.size()
        //val padding = size - (out.size() - before)
        //assert( padding >= 0 )
        //skipBytes( padding )
    }
}

final class FixedLengthString( var value : String ) extends FixedLengthSerializable
{
    def size = 32
    
    def this() = this("")
    
    override def saveImpl( out : DataOutput )
    {
        out.writeUTF( value )
    }
    
    override def loadImpl( in : DataInput )
    {
        value = in.readUTF()
    }
}


object FixedLengthString
{
    def size = 32
}

class EfficientArray[Element <: FixedLengthSerializable : Manifest]( var _length : Int ) extends ArrayLike[Element, EfficientArray[Element]]
{
    private def makeElement : Element = manifest[Element].erasure.newInstance.asInstanceOf[Element]
    private lazy val elementSize = makeElement.size
    
    private var buffer = new Array[Byte]( elementSize * length )
    
    class ArrayOutputStream( var index : Int ) extends OutputStream
    {
        override def write( b : Int )
        {
            buffer(index) = b.toByte
            index += 1
        }
    }
    
    class ArrayIterator() extends Iterator[Element]
    {
        var index = 0
        
        def hasNext = index < length * elementSize
        def next() =
        {
            val n = apply(index)
            index += 1
            n
        }
    }
    
    // No reason why this can't be more efficient
    class ArrayBuilder( val elementSize : Int ) extends Builder[Element, EfficientArray[Element]]
    {
        var data = new ArrayList[Element]()
        override def +=( elem : Element ) =
        {
            data.add( elem )
            this
        }
        override def clear() { data.clear() }
        override def result() =
        {
            val arr = new EfficientArray[Element]( data.size() )
            for ( i <- 0 until data.size() )
            {
                arr(i) = data.get(i)
            }
            arr
        }
    }

    override def newBuilder = new ArrayBuilder(elementSize)
    
    override def length : Int = _length
    override def apply( i : Int ) : Element =
    {
        val e : Element = makeElement
        e.load( new DataInputStream( new ByteArrayInputStream( buffer, i * elementSize, elementSize ) ) )
        e
    }
    
    override def update( i : Int, v : Element )
    {
        v.save( new DataOutputStream( new ArrayOutputStream( i * elementSize ) ) )
    }
    
    def getBuffer() = buffer
    def setBuffer( newBuffer : Array[Byte] ) { buffer = newBuffer }
    
    def save( file : File )
    {
        val stream = new FileOutputStream( file )
        stream.write( buffer )
    }
    
    def load( file : File )
    {
        val flen = file.length.toInt
        buffer = new Array[Byte]( flen )
        val stream = new FileInputStream( file )
        
        stream.read( buffer )
        _length = flen / elementSize
    }
}




class TextArrayWritable extends Writable
{
    var elements : List[String] = Nil
    var count = 0
    
    def append( value : String )
    {
        elements = value :: elements
        count += 1
    }
    
    override def write( out : DataOutput )
    {
        out.writeInt( count )
        for ( el <- elements ) out.writeUTF( el )
    }
    
    override def readFields( in : DataInput )
    {
        count = in.readInt
        elements = Nil
        for ( i <- 0 to (count-1) )
        {
            elements = in.readUTF :: elements
        }
    }
}

class TextArrayCountWritable extends Writable
{
    var elements : List[(String, Int)] = Nil
    var count = 0
    
    def append( value : String, number : Int )
    {
        elements = (value, number)::elements
        count += 1
    }
    
    override def write( out : DataOutput )
    {
        out.writeInt( count )
        for ( (value, number) <- elements )
        {
            out.writeUTF(value)
            out.writeInt(number)
        }
    }
    
    override def readFields( in : DataInput )
    {
        count = in.readInt
        elements = Nil
        for ( i <- 0 to (count-1) )
        {
            elements = (in.readUTF, in.readInt) :: elements
        }
    }
}

object Utils
{
    def normalize( raw : String ) : String =
    {
        // Deal with: double/triple spaces, embedded xml tags
        raw.toLowerCase().filter(_ != '\'' )
    }
    
    def normalizeTopicTitle( topicTitle : String ) : String =
    {
        if (topicTitle.contains(":")) topicTitle else "Main:" + topicTitle
    }
    
    def normalizeLink( destination : WikiTitle ) : String =
    {
        val namespace = destination.namespace.toString
        val destWithoutAnchor = destination.decoded.toString.split('#')(0)
        namespace + ":" + destWithoutAnchor
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
    
    def wikiParse( pageName : String, pageText : String ) : PageNode =
    {
        val markupParser = WikiParser()
        val title = WikiTitle.parse( pageName )
        val page = new WikiPage( title, 0, 0, pageText )
        markupParser( page )
    }
    
    def foldlWikiTree[T]( element : Node, startValue : T, fn : (Node, T) => T ) : T =
    {
        class FoldHelper( var iter : T, fn : (Node, T) => T )
        {
            def apply( element : Node ) : T =
            {
                iter = fn( element, iter )
                
                element match
                {
                    case InternalLinkNode(destination, children, line) => for ( child <- children ) apply( child )
                    case PageNode( title, id, revision, isRedirect, isDisambig, children ) => for ( child <- children ) apply( child )
                    case SectionNode( name, level, children, line ) => for ( child <- children ) apply( child )
                    case TemplateNode( title, children, line ) => for ( child <- children ) apply( child )
                    case TableNode( caption, children, line ) => for ( child <- children ) apply( child )
                    case TableRowNode( children, line ) => for ( child <- children ) apply( child )
                    case TableCellNode( children, line ) => for ( child <- children ) apply( child )
                    case PropertyNode( value, children, line ) => for ( child <- children ) apply( child )
                    case TextNode( text, line ) =>
                    
                    case _ =>
                }
                
                iter
            }
        }
        
        val fh = new FoldHelper( startValue, fn )
        fh.apply( element )
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
    
    // Be great if this were more generic
    def binarySearch[T <: FixedLengthSerializable](x: T, xs: EfficientArray[T], comp : (T, T)=> Boolean): Option[Int] =
    {
        def searchBetween(start: Int, end: Int): Option[Int] =
        {
            if ( start > end )
            {
                None
            }
            else
            {
                val pivot = (start + end) / 2
                val pivotValue = xs(pivot)
                
                if ( comp(x, pivotValue) )
                {
                    searchBetween(start, pivot-1)
                }
                else if ( comp(pivotValue, x) )
                {
                    searchBetween(pivot+1, end)
                }
                else
                {
                    Some( pivot )
                }
            }
        }

        searchBetween(0, xs.length-1)
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
                            val normalizedText = Utils.normalize( surfaceForm )

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
        Utils.traverseWikiTree( element, node => visitNode(node, contextAddFn) )
    }
}



