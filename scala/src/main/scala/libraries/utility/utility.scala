package org.seacourt.utility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Writable, Text, IntWritable}

import scala.collection.immutable.{TreeMap, HashSet, HashMap}


import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.ASCIIFoldingFilter

import java.io.{StringReader, OutputStream, InputStream}
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage

import scala.util.matching.Regex
import org.apache.hadoop.io.{Writable}
import java.io.{DataInput, DataOutput, FileInputStream, FileOutputStream, File}

import java.io.{DataOutput, DataOutputStream, DataInput, DataInputStream, ByteArrayInputStream}
import java.util.{ArrayList, Arrays}
import java.util.{TreeMap => JTreeMap, LinkedHashSet => JLinkedHashSet, HashMap => JHashMap}

import org.apache.commons.io.FileUtils.{deleteDirectory}

import scala.collection.mutable.{IndexedSeqOptimized, Builder, ArrayLike, LinkedList}


class AutoMap[A, B]( val makeB : A => B )
{
    private var mapImpl = HashMap[A, B]()
    
    def apply( key : A ) =
    {
        if ( mapImpl.contains(key) )
        {
            mapImpl(key)
        }
        else
        {
            val newB = makeB( key )
            mapImpl = mapImpl.updated( key, newB )
            newB
        }
    }
    
    def remove( key : A )
    {
        mapImpl = mapImpl - key
    }
    
    def set( key : A, value : B )
    {
        mapImpl = mapImpl.updated( key, value )
    }
    
    def contains = mapImpl.contains _
    def foreach = mapImpl.foreach _
    def filter = mapImpl.filter _
    def map[C]( fn : ((A, B)) => C ) = mapImpl.map( fn )
    def toList = mapImpl.toList
}

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

final class EfficientIntPair( var first : Int, var second : Int ) extends FixedLengthSerializable
{
    def size = 8
    
    def this() = this(0, 0)
    
    override def saveImpl( out : DataOutput )
    {
        out.writeInt( first )
        out.writeInt( second )
    }
    
    override def loadImpl( in : DataInput )
    {
        first = in.readInt()
        second = in.readInt()
    }
    
    def less( other : EfficientIntPair ) : Boolean =
    {
        if ( first != other.first ) first < other.first
        else second < other.second
    }
}

final class EfficientIntIntDouble( var first : Int, var second : Int, var third : Double ) extends FixedLengthSerializable
{
    def size = 16
    
    def this() = this(0, 0, 0.0)
    
    override def saveImpl( out : DataOutput )
    {
        out.writeInt( first )
        out.writeInt( second )
        out.writeDouble( third )
    }
    
    override def loadImpl( in : DataInput )
    {
        first = in.readInt()
        second = in.readInt()
        third = in.readDouble()
    }
    
    def less( other : EfficientIntIntDouble ) : Boolean =
    {
        if ( first != other.first ) first < other.first
        else if ( second != other.second ) second < other.second
        else third < other.third
    }
}

final class EfficientIntIntInt( var first : Int, var second : Int, var third : Int ) extends FixedLengthSerializable
{
    def size = 12
    
    def this() = this(0, 0, 0)
    
    override def saveImpl( out : DataOutput )
    {
        out.writeInt( first )
        out.writeInt( second )
        out.writeInt( third )
    }
    
    override def loadImpl( in : DataInput )
    {
        first = in.readInt()
        second = in.readInt()
        third = in.readInt()
    }
    
    def less( other : EfficientIntIntInt ) : Boolean =
    {
        if ( first != other.first ) first < other.first
        else if ( second != other.second ) second < other.second
        else third < other.third
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
    
    class ArrayInputStream( var index : Int ) extends InputStream
    {
        override def read() : Int =
        {
            if ( index >= buffer.size )
            {
                -1
            }
            else
            {
                val v = buffer(index).toInt & 0xff
                index += 1
                
                v
            }
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
    class ArrayBuilder() extends Builder[Element, EfficientArray[Element]]
    {
        var data = new EfficientArray[Element]( 64 )
        var appendIndex = 0
        
        override def +=( elem : Element ) =
        {
            if ( appendIndex == (data.length-1) )
            {
                data.resize( (data.length * 3)/2 )
            }
            data(appendIndex) = elem
            appendIndex += 1

            this
        }
        override def clear() { data.clear() }
        override def result() =
        {
            data.resize( appendIndex )
            data
        }
    }
    
    def truncate( newLength : Int )
    {
        assert( newLength <= length )
        _length = newLength
    }

    override def newBuilder = new ArrayBuilder()
    
    val is = new ArrayInputStream( 0 )
    val idis = new DataInputStream(is)
    
    val os = new ArrayOutputStream(0)
    val odos = new DataOutputStream(os)
    
    override def length : Int = _length
    override def apply( i : Int ) : Element =
    {
        val e : Element = makeElement
        is.index = i * elementSize
        e.load(idis)
        e
    }
    
    override def update( i : Int, v : Element )
    {
        os.index = i * elementSize
        v.save( odos )
    }
    
    def getBuffer() = buffer
    def setBuffer( newBuffer : Array[Byte] ) { buffer = newBuffer }
    
    def resize( numElements : Int )
    {
        buffer = Arrays.copyOf( buffer, numElements * elementSize )
        _length = numElements
    }
    
    def clear() = resize(0)
    
    def save( outStream : DataOutputStream )
    {
        outStream.writeInt( _length * elementSize )
        outStream.write( buffer )
    }
    
    def load( inStream : DataInputStream )
    {
        val length = inStream.readInt()
        buffer = new Array[Byte]( length )
        inStream.read( buffer)
        _length = length / elementSize
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

object TextUtils
{
    def normalize( raw : String ) : String =
    {
        // Deal with: double/triple spaces, embedded xml tags
        raw.toLowerCase().filter(_ != '\'' ).map( x => if (x =='/' || x == '-') ' ' else x )
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
        
    def luceneTextTokenizer( _page : String ) : List[String] =
    {
        var page = normalize(_page)
        
        page = page.replace( "c++", "cplusplus" ).replace( ".net", "dotnet" )
    	val textSource = new StringReader( page )
    	
    	// Consider using org.apache.lucene.analysis.standard.StandardAnalyzer instead as it filters out 's, moves to lower case, removes stop words.
    	// Add ASCIIFoldingFilter to fold weird punctation down to ascii
    	// And org.apache.lucene.analysis.PorterStemFilter for porter stemming? Not clear how useful stemming is.
        val tokenizerBase = new StandardTokenizer( LUCENE_30, textSource )
        val tokenizer = new ASCIIFoldingFilter( tokenizerBase )
    
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
    
    def normalizeSF( in : String ) : String =
    {
        val rawNormed = normalize(in)
        val els = luceneTextTokenizer( rawNormed )
        
        els.mkString(" ")
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
}

object Utils
{
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
    
    def lowerBound[T <: FixedLengthSerializable](x: T, xs: EfficientArray[T], comp : (T, T)=> Boolean): Int =
    {
        def searchBetween(start: Int, end: Int): Int =
        {
            val pivot = (start + end) / 2
            val pivotValue = xs(pivot)
            
            //println( ":: " + start + ", " + end + ": " + x + ", " + pivotValue )
            
            val ls = comp(x, pivotValue)
            val gt = comp(pivotValue, x)
            
            if ( ls )
            {
                if ( start == pivot ) start
                else searchBetween(start, (pivot-1))
            }
            else if ( gt )
            {
                if ( pivot == end ) start+1
                else searchBetween(pivot+1, end)
            }
            else
            {
                pivot
            }
        }

        searchBetween(0, xs.length-1)
    }
    
    def withTemporaryDirectory( code : String => Unit )
    {
        val dirStr = "temp" + System.nanoTime().toString()
        val dir = new File(dirStr)
        dir.mkdir()
        try
        {
            code(dirStr)
        }
        finally
        {
            deleteDirectory( dir )
        }
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
                            val normalizedText = TextUtils.normalize( surfaceForm )

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
        TextUtils.traverseWikiTree( element, node => visitNode(node, contextAddFn) )
    }
}

trait WrappedWritable[From <: Writable, To]
{
    val writable : From
    def get : To
}

final class WrappedString extends WrappedWritable[Text, String]
{
    val writable = new Text() 
    def get = writable.toString()
}

final class WrappedInt extends WrappedWritable[IntWritable, Int]
{
    val writable = new IntWritable() 
    def get = writable.get()
}

final class WrappedTextArrayWritable extends WrappedWritable[TextArrayWritable, List[String]]
{
    val writable = new TextArrayWritable()
    def get = writable.elements
}

final class WrappedTextArrayCountWritable extends WrappedWritable[TextArrayCountWritable, List[(String, Int)]]
{
    val writable = new TextArrayCountWritable()
    def get = writable.elements
}

class SeqFilesIterator[KeyType <: Writable, ValueType <: Writable, ConvKeyType, ConvValueType]( val conf : Configuration, val fs : FileSystem, val basePath : String, val seqFileName : String, val keyField : WrappedWritable[KeyType, ConvKeyType], val valueField : WrappedWritable[ValueType, ConvValueType] ) extends Iterator[(ConvKeyType, ConvValueType)]
{
    var fileList = getJobFiles( fs, basePath, seqFileName )
    var currFile = advanceFile()
    private var _hasNext = advance( keyField.writable, valueField.writable )
    
    private def getJobFiles( fs : FileSystem, basePath : String, directory : String ) =
    {
        val fileList = fs.listStatus( new Path( basePath + "/" + directory ) ).toList
        
        fileList.map( _.getPath ).filter( !_.toString.endsWith( "_SUCCESS" ) )
    }
    
    private def advanceFile() : HadoopReader =
    {
        if ( fileList == Nil )
        {
            null
        }
        else
        {
            println( "Reading file: " + fileList.head )
            val reader = new HadoopReader( fs, fileList.head, conf )
            fileList = fileList.tail
            reader
        }
    }
    
    private def advance( key : KeyType, value : ValueType ) : Boolean =
    {
        var success = currFile.next( key, value )
        if ( !success )
        {
            currFile = advanceFile()
            
            if ( currFile != null )
            {
                success = currFile.next( key, value )
            }
        }
        success
    }
    
    
    override def hasNext() : Boolean = _hasNext
    override def next() : (ConvKeyType, ConvValueType) =
    {
        val current = (keyField.get, valueField.get)
        _hasNext = advance( keyField.writable, valueField.writable )
        
        current
    }
}

class NPriorityQ[V]()
{
    type K = Double
    private val container = new JTreeMap[K, JLinkedHashSet[V]]()
    private var numElements = 0
    
    def add( k : K, v : V )
    {
        if ( container.containsKey(k) )
        {
            val old = container.get(k)
            
            if ( !old.contains(v) )
            {
                old.add(v)
                numElements += 1
            }
        }
        else
        {
            val next = new JLinkedHashSet[V]()
            next.add(v)
            container.put( k, next )
            numElements += 1
        }
    }
    
    def remove( k : K, v : V )
    {
        assert( container.containsKey(k) )
        
        val el = container.get(k)
        el.remove(v)
        
        if ( el.isEmpty() )
        {
            container.remove(k)
        }
        numElements -= 1
    }
    
    def first() =
    {
        //println( numElements + " " + container.isEmpty() )
        //assert( numElements > 0 )
        assert( !container.isEmpty() )
        val firstKey = container.firstKey()
        val firstVal = container.get(firstKey)
        val el = firstVal.iterator().next()
        (firstKey, el)
    }

    def size = numElements
    
    def popFirst() : (K, V) =
    {
        val res = first()
        remove( res._1, res._2 )
        
        res
    }
    
    def isEmpty = (numElements == 0)
}

class PriorityQ[V]()
{    
    type K = Double
    private var container = TreeMap[K, HashSet[V]]()
    var numElements = 0
    
    def add( k : K, v : V )
    {
        val el = container.getOrElse( k, HashSet[V]() )
        container = container.updated( k, el + v )
        numElements += 1
    }
    
    def size = numElements
    
    def remove( k : K, v : V )
    {
        assert( container.contains(k) )
        val el = container(k)
        if ( el.size == 1 )
        {
            container = container - k
        }
        else
        {
            assert( container(k).contains(v) )
            container = container.updated( k, el - v )
        }   
        numElements -= 1
    }
    def first() : (K, V) =
    {
        val h = container.head
        
        (h._1, h._2.head)
    }
    
    def popFirst() : (K, V) =
    {
        val h = container.head
        val res = (h._1, h._2.head)
        remove( res._1, res._2 )
        
        res
    }
    
    def isEmpty : Boolean = container.isEmpty
}


class DisjointSet[T]( val value : T )
{
    type Self = DisjointSet[T]
    
    var parent = this
    var rank = 0
    var setsize = 1
    var children = List[Self]()
    
    private def getChildren() : List[Self] =
    {
        children.foldLeft( List(this) )( (l, c) => l ++ c.getChildren() )
    }
    
    def setParent( p : Self )
    {
        parent = p
        parent.setsize += setsize
        //parent.details.join( details )
        parent.children = this :: parent.children
    }
    
    def join( rhs : Self )
    {
        assert( this != rhs )
        val xroot = find()
        val yroot = rhs.find()
        
        assert( xroot != yroot )
        if ( xroot.rank > yroot.rank ) yroot.setParent( xroot )
        else if ( xroot.rank < yroot.rank ) xroot.setParent( yroot )
        else if ( xroot != yroot )
        {
            yroot.setParent( xroot )
            xroot.rank += 1
        }
    }
    
    def equals( rhs : Self ) = find() == rhs.find()
    def find() : Self = if ( parent != this ) parent.find() else this
    def get() = find().value
    def size() = find().setsize
    def members() = find().getChildren()
}


