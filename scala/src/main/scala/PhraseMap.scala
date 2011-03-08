import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.util.TreeMap
import butter4s.json.Parser

class PhraseNode[TargetType]
{
    type SelfType = PhraseNode[TargetType]
    val children = new TreeMap[Char, SelfType]()
    var terminalData : List[TargetType] = Nil
    var terminalCount = 0
    
    def add( phrase : Seq[Char], endpoint : TargetType )
    {
        phrase match
        {
            case Seq( head, tail @ _* ) =>
            {
                if ( !children.containsKey(head) )
                {
                    children.put(head, new SelfType())
                }
                
                children.get(head).add(tail, endpoint)
            }
            case Seq() =>
            {
                terminalData = endpoint::terminalData
                terminalCount += 1
                if ( terminalCount > 1000 )
                {
                   println( terminalCount )
                }
            }
        }
    }
    
    def walk( el : Char ) : Option[SelfType] =
    {
        if ( children.containsKey( el ) )
        {
            return Some( children.get(el) )
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
                val parsed = Parser.parse( references )
                parsed match
                {
                    case elMap : Map[Int, String] =>
                    {
                        for ( (index, value) <- elMap )
                        {
                            pm.addPhrase( surfaceForm, value )
                        }
                        
                        count += 1
                        
                        if ( count % 100000 == 0 )
                        {
                            println( count + " " + surfaceForm + " -> " + elMap.toString() )
                        }
                    }
                    case _ =>
                }
                
                
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
        val path5 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00005")
        val path6 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00006")
        val path7 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00007")
        val path8 = new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres/part-r-00008")
        
        
        
        parseFile( pm, fs, path0 )
        parseFile( pm, fs, path1 )
        parseFile( pm, fs, path2 )
        parseFile( pm, fs, path3 )
        parseFile( pm, fs, path4 )
        parseFile( pm, fs, path5 )
        parseFile( pm, fs, path6 )
        parseFile( pm, fs, path7 )
        parseFile( pm, fs, path8 )
        
        println( "*** Parse complete ***" )
        
        val r = Runtime.getRuntime()
        r.gc()
        
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

