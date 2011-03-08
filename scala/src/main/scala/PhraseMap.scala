import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.util.{TreeMap, HashMap}
import butter4s.json.Parser

import java.io.File
import com.almworks.sqlite4java._

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
    
    
    //private def parseFile( pm : PhraseMap[String], fs : FileSystem, path : Path )
    private def parseFile( consumeFn : (String, String) => Unit, fs : FileSystem, path : Path )
    {
        assert( fs.exists(path) )
        val reader = new BufferedReader( new InputStreamReader( fs.open( path ) ) )
        
        var count = 0
        var line : String = null
        while ( {line = reader.readLine(); line != null} )
        {
            val firstTabIndex = line.indexOf( '\t' )
            val surfaceForm = new String(line.substring(0,firstTabIndex))
            val references = new String(line.substring(firstTabIndex+1))
            
            try
            {
                val parsed = Parser.parse( references )
                parsed match
                {
                    case elMap : Map[Int, String] =>
                    {
                        for ( (index, value) <- elMap )
                        {
                            //pm.addPhrase( surfaceForm, value )
                            consumeFn( surfaceForm, value )
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
    
    class SQLiteWriter( fileName : String )
    {
        val db = new SQLiteConnection( new File(fileName) )
        val addStmt = db.prepare( "INSERT INTO surfaceForms VALUES( ?, ? )" )
        var count = 0
        
        db.open()
        db.exec( "CREATE TABLE surfaceForms( form TEXT, topic TEXT )" )
        
        def addPhrase( surfaceForm : String, topic : String )
        {
            if ( count == 0 )
            {
                db.exec( "BEGIN" )
            }
            addStmt.bind(1, surfaceForm)
            addStmt.bind(2, topic)
            addStmt.step()
            
            if ( count == 0 )
            {
                db.exec( "COMMIT" )
            }
            
            if ( count < 10000 )
            {
                count += 1
            }
            else
            {
                count = 0
            }
        }
    }

    def main( args : Array[String] )
    {
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        val fs = FileSystem.get(conf)   
        
        
        val pm = new PhraseMap[String]()
        val sql = new SQLiteWriter( "test.sqlite3" )
        
        val fileList = fs.listStatus( new Path( "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres" ) )
        
        
        for ( fileStatus <- fileList )
        {
            val filePath = fileStatus.getPath
            println( filePath )
            parseFile( sql.addPhrase, fs, filePath )
            //parseFile( pm.addPhrase, fs, filePath )
        }
        
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

