import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.util.{TreeMap, HashMap, HashSet}
import butter4s.json.Parser

import java.io.File

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import SqliteWrapper._

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
                            //println( "    " + count + " " + surfaceForm )
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
        //val db = new SQLiteConnection( new File(fileName) )
        val db = new SQLiteWrapper( new File( fileName ) )
        
        val topics = new HashSet[String]
        
        // Various options, inc. 2Gb page cache and no journal to massively speed up creation of this db
        db.exec( "PRAGMA cache_size=512000" )
        db.exec( "PRAGMA journal_mode=off" )
        db.exec( "PRAGMA synchronous=off" )
        db.exec( "CREATE TABLE topics( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)" )
        db.exec( "CREATE TABLE words( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, UNIQUE(name))" )
        db.exec( "CREATE TABLE phraseTreeNodes( id INTEGER PRIMARY KEY, parentId INTEGER, wordId INTEGER, FOREIGN KEY(parentId) REFERENCES phrases(id), FOREIGN KEY(wordId) REFERENCES words(id), UNIQUE(parentId, wordId) )" )
        db.exec( "CREATE TABLE phraseTopics( phraseTreeNodeId INTEGER, topicId INTEGER, FOREIGN KEY(phraseTreeNodeId) REFERENCES phraseTreeNodes(id), FOREIGN KEY(topicId) REFERENCES topics(id) )" )
        db.exec( "CREATE TABLE categories( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)" )
        db.exec( "CREATE TABLE categoryMembership( topicId INTEGER, categoryId INTEGER, FOREIGN KEY(topicId) REFERENCES topics(id), FOREIGN KEY(categoryId) REFERENCES topics(id) )" )
        
        val addTopic = db.prepare( "INSERT INTO topics VALUES( NULL, ? )", HNil )
        //val addSurfaceForm = db.prepare( "INSERT INTO surfaceForms SELECT ?, id FROM topics WHERE topics.name=?" )
        val addWord = db.prepare( "INSERT OR IGNORE INTO words VALUES(NULL, ?)", HNil )
        
        val getExistingTreeNode = db.prepare( "SELECT id FROM phraseTreeNodes WHERE parentId=? AND wordId=(SELECT id FROM words WHERE name=?)", Col[Int]::HNil )
        val addTreeNode = db.prepare( "INSERT INTO phraseTreeNodes VALUES( NULL, ?, (SELECT id FROM words WHERE name=?) )", HNil )
        
        val addPhraseTopic = db.prepare( "INSERT INTO phraseTopics VALUES( ?, (SELECT id FROM topics WHERE name=?) )", HNil )
        val addCategory = db.prepare( "INSERT INTO categories VALUES( NULL, ? )", HNil )
        val addCategoryMembership = db.prepare( "INSERT INTO categoryMembership VALUES( (SELECT id FROM topics WHERE name=?), (SELECT id FROM categories WHERE name=?) )", HNil )
        
        var count = 0
        
        db.exec( "BEGIN" )
        
        def exec( stmt : String )
        {
            db.exec( stmt )
        }
        
        private def manageTransactions()
        {
            count += 1
            if ( (count % 100000) == 0 )
            {
                println( "Committing " + count )
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
                addTopic.exec( trimmedTopic )
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
        
        def addWords( surfaceForm : String, topic : String )
        {
            val words = getWords( new StringReader( surfaceForm ) )
            for ( word <- words ) addWord.exec( word )
            
            manageTransactions() 
        }
        
        def addPhrase( surfaceForm : String, topic : String )
        {
            val words = getWords( new StringReader( surfaceForm ) )
            
            //println( words.toString() )

            var parentId = -1L
            var count = 0
            for ( word <- words )
            {
                var treeNodeId = 0L
                getExistingTreeNode.bind( parentId, word )
                
                val res = getExistingTreeNode.toList
                if ( res != Nil )
                {
                    treeNodeId = _1(res.head).get
                }
                else
                {
                    addTreeNode.exec(parentId, word)
                    treeNodeId = db.getLastInsertId()
                    //addTreeNode.reset()
                }
                getExistingTreeNode.reset()
                parentId = treeNodeId
                count += 1
            }
            
            val trimmedTopic = topic.split("::")(1).trim()
            addPhraseTopic.exec( parentId, trimmedTopic )
            
            manageTransactions()
        }

        def addCategory( topicName : String, categoryName : String )
        {
            if ( !topics.contains(categoryName) )
            {
                addCategory.exec(categoryName)
                
                topics.add( categoryName )
                
                manageTransactions()
            }
        }
        
        def addCategoryMapping( topicName : String, categoryName : String )
        {
            addCategoryMembership.exec(topicName, categoryName)
            
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
        run( conf, args(0), args(1) )
    }
    
    def run( conf : Configuration, inputDataDirectory : String, outputFilePath : String )
    {
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        val fs = FileSystem.get(conf)   
        
        val sql = new SQLiteWriter( outputFilePath )
        
        val basePath = "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/" + inputDataDirectory;
        
        {
            println( "Adding topics..." )
            val fileList = fs.listStatus( new Path( basePath + "/surfaceforms" ) )
            
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addTopic, fs, filePath )
            }
            
            sql.clearTopicMap()
            
            println( "Creating topic index..." )
            sql.exec( "CREATE INDEX topicNameIndex ON topics(name)" )
            
            println( "Adding words..." )
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addWords, fs, filePath )
            }
            
            println( "Adding surface forms..." )
            for ( fileStatus <- fileList )
            {
                val filePath = fileStatus.getPath
                println( filePath )
                parseFile( sql.addPhrase, fs, filePath )
            }
            
            println( "Creating surface forms index..." )
            sql.exec( "CREATE INDEX phraseTopicIndex ON phraseTopics(phraseTreeNodeId)" )
        }
        
        
        {
            val fileList = fs.listStatus( new Path( basePath + "/categoryMembership" ) )
            
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
    }
}

