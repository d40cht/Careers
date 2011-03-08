import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.immutable.TreeMap

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

