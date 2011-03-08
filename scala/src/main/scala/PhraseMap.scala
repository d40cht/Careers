import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.immutable.TreeMap

abstract class PhraseTreeElement

case class Node() extends PhraseTreeElement
{
    var children = new TreeMap[String, PhraseTreeElement]()
    
    def addPhrase( phrase : Seq[Char] )
    {
        match phrase
        {
            case head::tail =>
            {
                var children = children.add( addPhrase( tail ) )
            }
            case Nil =>
        }
    }
   
    def walk( el : Char )
    {
    }
}
case class Leaf() extends PhraseTreeElement


class PhraseMap
{
    val root = new Node()
   
    
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

