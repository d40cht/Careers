import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.util.{TreeMap, HashMap, HashSet}
import butter4s.json.Parser

import java.io.File
import com.almworks.sqlite4java._

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

/*class PhraseNode[TargetType]
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
}*/

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
                            consumeFn( surfaceForm, value )
                        }
                        
                        count += 1
                        
                        if ( count % 10000 == 0 )
                        {
                            println( "    " + count + " " + surfaceForm )
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
        
        val topics = new HashSet[String]
        
        db.open()
        // Various options, inc. 2Gb page cache and no journal to massively speed up creation of this db
        db.exec( "PRAGMA cache_size=512000" )
        db.exec( "PRAGMA journal_mode=off" )
        db.exec( "PRAGMA synchronous=off" )
        db.exec( "CREATE TABLE topics( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)" )
        db.exec( "CREATE TABLE words( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, UNIQUE(name))" )
        db.exec( "CREATE TABLE phraseTreeNodes( id INTEGER PRIMARY KEY, parentId INTEGER, wordId INTEGER, FOREIGN KEY(parentId) REFERENCES phrases(id), FOREIGN KEY(wordId) REFERENCES words(id)" )
        db.exec( "CREATE TABLE phraseTopics( phraseTreeNodeId INTEGER, topicId INTEGER, FOREIGN KEY(phraseTreeNodeId) REFERENCES phraseTreeNodes(id), FOREIGN KEY(topicId) REFERENCES topics(id)" )
        db.exec( "CREATE TABLE categories( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)" )
        db.exec( "CREATE TABLE categoryMembership( topicId INTEGER, categoryId INTEGER, FOREIGN KEY(topicId) REFERENCES topics(id), FOREIGN KEY(categoryId) REFERENCES topics(id) )" )
        
        val addTopic = db.prepare( "INSERT INTO topics VALUES( NULL, ? )" )
        val addSurfaceForm = db.prepare( "INSERT INTO surfaceForms SELECT ?, id FROM topics WHERE topics.name=?" )
        val addWord = db.prepare( "INSERT OR IGNORE INTO words VALUES(NULL, ?)" )
        
        val getExistingTreeNode = db.prepare( "SELECT id FROM testTree WHERE parentId=? AND value=(SELECT id FROM words WHERE name=?)" )
        val addTreeNode = db.prepare( "INSERT INTO testTree VALUES( NULL, ?, (SELECT id FROM words WHERE name=?) )" )
        
        val addPhraseTopic = db.prepare( "INSERT INTO phraseTopics VALUES( ?, (SELECT id FROM topics WHERE topics.name=?) ) )" )
        val addCategory = db.prepare( "INSERT INTO categories VALUES( NULL, ? )" )
        val addCategoryMembership = db.prepare( "INSERT INTO categoryMembership VALUES( (SELECT id FROM topics WHERE name=?), (SELECT id FROM categories WHERE name=?) )" )
        
        var count = 0
        
        db.exec( "BEGIN" )
        
        def exec( stmt : String )
        {
            db.exec( stmt )
        }
        
        private def manageTransactions()
        {
            count += 1
            if ( (count % 10000) == 0 )
            {
                db.exec( "COMMIT" )
                db.exec( "BEGIN" )
            }
        }
        
        def clearTopicMap()
        {
            topics.clear()
        }
        
        def addTopic( surfaceForm : String, topic : String )
        {
            val trimmedTopic = topic.split("::")(1).trim()
            if ( !topics.contains( topic ) )
            {
                addTopic.bind(1, trimmedTopic )
                addTopic.step()
                addTopic.reset()
                
                topics.add( topic )
                
                manageTransactions()
            }
        }
        
        def getWords( textSource : Reader ) : List[String] =
        {
            val tokenizer = new StandardTokenizer( LUCENE_30, textSource )
        
            var run = true
            var wordList : List[String] = Nil
            while ( run )
            {
                wordList = tokenizer.getAttribute(classOf[TermAttribute]).term() :: wordList
                run = tokenizer.incrementToken()
            }
            tokenizer.close()
            
            return wordList
        }
        
        def addWords( surfaceForm : String, topic : String )
        {
            val words = getWords( new StringReader( surfaceForm ) )
            for ( word <- words )
            {
                addWord.bind(1, word)
                addWord.step()
                addWord.reset()
            }   
        }
        
        def addPhrase( surfaceForm : String, topic : String )
        {
            val words = getWords( new StringReader( surfaceForm ) )
            var parentId = -1L
            for ( word <- words )
            {
                var treeNodeId = 0L
                getExistingTreeNode.bind( 1, parentId )
                getExistingTreeNode.bind( 2, word )
                getExistingTreeNode.step()
                if ( getExistingTreeNode.step() )
                {
                    treeNodeId = getExistingTreeNode.columnInt(0)
                }
                else
                {
                    addTreeNode.bind(1, parentId)
                    addTreeNode.bind(2, word)
                    addTreeNode.step()
                    treeNodeId = db.getLastInsertId()
                    addTreeNode.reset()
                }
                getExistingTreeNode.reset()
                parentId = treeNodeId
            }
            
            addPhraseTopic.bind(1, parentId)
            addPhraseTopic.bind(2, topic)
            addPhraseTopic.step()
            addPhraseTopic.reset()
        }

        def addCategory( topicName : String, categoryName : String )
        {
            if ( !topics.contains(categoryName) )
            {
                addCategory.bind(1, categoryName)
                addCategory.step()
                addCategory.reset()
                
                topics.add( categoryName )
                
                manageTransactions()
            }
        }
        
        def addCategoryMapping( topicName : String, categoryName : String )
        {
            addCategoryMembership.bind(1, topicName)
            addCategoryMembership.bind(2, categoryName)
            addCategoryMembership.step()
            addCategoryMembership.reset()
            
            manageTransactions()
        }
        
        def close()
        {
            db.exec( "COMMIT" )
        }
    }

    def main( args : Array[String] )
    {
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        val fs = FileSystem.get(conf)   
        
        
        //val pm = new PhraseMap[String]()
        val sql = new SQLiteWriter( "test.sqlite3" )
        
        {
            println( "Adding topics..." )
            val fileListTemp = fs.listStatus( new Path( "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/surfaceformres" ) )
            val fileList = List(fileListTemp(0))
            
            
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addTopic, fs, filePath )
            }
            
            sql.clearTopicMap()
            
            println( "Creating topic index..." )
            sql.exec( "CREATE INDEX topicNameIndex ON topics(name)" )
            
            println( "Adding surface forms..." )
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addPhrase, fs, filePath )
            }
            
            println( "Creating surface forms index..." )
            sql.exec( "CREATE INDEX surfaceFormsIndex on surfaceForms(name)" )
        }
        
        
        {
            val fileList = fs.listStatus( new Path( "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/categoryres" ) )
            
            println( "Adding categories..." )
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addCategory, fs, filePath )
            }
            
            sql.clearTopicMap()
            
            println( "Building category index..." )
            sql.exec( "CREATE INDEX categoryIndex ON categories(name)" )
            
            println( "Adding category mappings..." )
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addCategoryMapping, fs, filePath )
            }
            
            println( "Building category membership index..." )
            sql.exec( "CREATE INDEX categoryMembershipIndex ON categoryMembership(topicId)" )
        }
        
        sql.close()
        println( "*** Parse complete ***" )

        /*def printRes( m : String, terminals : List[String] )
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
        }*/
    }
}

