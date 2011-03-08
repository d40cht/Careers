import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.immutable.TreeMap

import edu.umd.cloud9.io.JSONObjectWritable
import org.json.JSONException

class PhraseNode[TargetType]
{
    type SelfType = PhraseNode[TargetType]
    var children = new TreeMap[Char, SelfType]()
    var terminalData : List[TargetType] = Nil
    
    def add( phrase : Seq[Char], endpoint : TargetType )
    {
        phrase match
        {
            case Seq( head, tail @ _* ) =>
            {
                if ( !children.contains(head) )
                {
                    children += (head -> new SelfType())
                }
                
                children(head).add(tail, endpoint)
            }
            case Seq() =>
            {
                terminalData = endpoint::terminalData
            }
        }
    }
    
    def walk( el : Char ) : Option[SelfType] =
    {
        if ( children.contains( el ) )
        {
            return Some( children(el) )
        }
        else
        {
            return None
        }
    }
}

class PhraseMap[TargetType]
{
    val root = new PhraseNode[TargetType]()
   
    
    def addPhrase( phrase : String, endpoint : TargetType )
    {
        root.add( phrase, endpoint )
    }
}

class PhraseWalker[TargetType]( val phraseMap : PhraseMap[TargetType], val phraseRegFn : (String, List[TargetType]) => Unit )
{
    type PhraseListType = List[(List[Char], PhraseNode[TargetType])]
    
    var activePhrases : PhraseListType = Nil

    def startNew()
    {
        activePhrases = (List[Char](), phraseMap.root)::activePhrases
    }

    private def phrasesUpdateImpl( el : Char, activeList : PhraseListType ) : PhraseListType =
    {
        activeList match
        {
            case (headChars, headPhraseNode)::tail =>
            {
                if ( headPhraseNode.terminalData != Nil )
                {
                    if ( PhraseMap.isNonWordChar( el ) )
                    {
                        phraseRegFn( headChars.reverse.mkString(""), headPhraseNode.terminalData )
                    }
                }
                
                headPhraseNode.walk(el) match
                {
                    case Some( rest ) =>
                    {
                        return (el::headChars, rest)::phrasesUpdateImpl( el, tail )
                    }
                    case None =>
                    {
                        return phrasesUpdateImpl( el, tail )
                    }
                }
            }
            case Nil =>
            {
                return Nil
            }
        }
    }
    
    def update( el : Char )
    {
        activePhrases = phrasesUpdateImpl( el, activePhrases )
    }
}

// Helper to allow iterating over java util iterators
class IteratorWrapper[A](iter:java.util.Iterator[A])
{
    def foreach(f: A => Unit): Unit = {
        while(iter.hasNext){
          f(iter.next)
        }
    }
}

object PhraseMap
{
    implicit def iteratorToWrapper[T](iter:java.util.Iterator[T]):IteratorWrapper[T] = new IteratorWrapper[T](iter)
     
    def isNonWordChar( el : Char ) : Boolean =
    {
        el match
        {
            case ' ' => return true
            case ',' => return true
            case ';' => return true
            case '.' => return true
            case ':' => return true
            case '!' => return true
            case '?' => return true
            case _ => return false
        }
    }
    
    
    private def parseFile( pm : PhraseMap[String], fs : FileSystem, path : Path )
    {
        assert( fs.exists(path) )
        val reader = new BufferedReader( new InputStreamReader( fs.open( path ) ) )
        
        var count = 0
        var line : String = null
        while ( {line = reader.readLine(); line != null} )
        {
            val firstTabIndex = line.indexOf( '\t' )
            val surfaceForm = line.substring(0,firstTabIndex)
            val references = line.substring(firstTabIndex+1)
            
            try
            {
                val parsed = new JSONObjectWritable( references )
                
                var kk = 0
                for ( key <- parsed.keys() )
                {
                    pm.addPhrase( surfaceForm, parsed.getString(key) )
                    kk += 1
                }
                
                if ( kk > 40 )
                {
                    //println( count + " == " + line )
                }
                count += 1
            }
            catch
            {
                case e : org.json.JSONException =>
                {
                    println( "JSON exception parsing: " + line )
                }
                case e : Exception =>
                {
                    println( "General exception: " + e.toString )
                }
            }
        }
        reader.close()
    }

    def main( args : Array[String] )
    {
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        val fs = FileSystem.get(conf)   
        
        
        val pm = new PhraseMap[String]()
        
        val path0 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00000")
        val path1 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00001")
        val path2 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00002")
        val path3 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00003")
        val path4 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00004")
        
        
        parseFile( pm, fs, path0 )
        parseFile( pm, fs, path1 )
        parseFile( pm, fs, path2 )
        parseFile( pm, fs, path3 )
        parseFile( pm, fs, path4 )
        
        def printRes( m : String, terminals : List[String] )
        {
            println( "Found: " + m + ", " + terminals.toString() )
        }
        
        while ( true )
        {
            val query = Console.readLine
            val pw = new PhraseWalker( pm, printRes )
            var lastChar = ' '
            for (c <- query )
            {
                if ( PhraseMap.isNonWordChar(lastChar) )
                {
                    pw.startNew()
                    
                }
                pw.update( c )
                lastChar = c
            }
            pw.update( ' ' )
        }
    }
}

