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
        root.add( phrase )
    }
}

class PhraseWalker( val phraseMap : PhraseMap, val phraseRegFn : String => Unit )
{
    type PhraseListType = List[(List[Char], PhraseNode)]
    
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
                if ( headPhraseNode.isTerminal )
                {
                    if ( PhraseMap.isNonWordChar( el ) )
                    {
                        phraseRegFn( headChars.reverse.mkString("") )
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


object PhraseMap
{
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

    def main( args : Array[String] )
    {
        val path = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres")
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
    }
}

