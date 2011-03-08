import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.immutable.TreeMap

class PhraseNode()
{
    var children = new TreeMap[Char, PhraseNode]()
    var isTerminal = false
    
    def add( phrase : Seq[Char] )
    {
        phrase match
        {
            case Seq( head, tail @ _* ) =>
            {
                if ( !children.contains(head) )
                {
                    children += (head -> new PhraseNode())
                }
                
                children(head).add(tail)
            }
            case Seq() =>
            {
                isTerminal = true
            }
        }
    }
    
    def walk( el : Char ) : Option[PhraseNode] =
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

class PhraseMap
{
    val root = new PhraseNode()
   
    
    def addPhrase( phrase : String )
    {
        val z : Seq[Char] = phrase
        root.add( phrase )
    }
}


object PhraseMap
{
    def main( args : Array[String] )
    {
        val path = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres")
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        
        
    }
}

