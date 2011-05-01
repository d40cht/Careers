import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io.SequenceFile.{createWriter, Reader => HadoopReader}
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.util.{TreeMap, HashMap, HashSet}

import java.io.File

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._

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
            
            // TODO: JSON is out. Use Writable derived objects in utility
            /*try
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
            }*/
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

        db.exec( "CREATE TABLE topics( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, UNIQUE(name))" )
        db.exec( "CREATE TABLE redirects( fromId INTEGER, toId INTEGER, FOREIGN KEY(fromId) REFERENCES topics(id), FOREIGN KEY(toId) REFERENCES topics(id), UNIQUE(fromId) )" )
        db.exec( "CREATE TABLE words( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, count INTEGER, UNIQUE(name))" )
        db.exec( "CREATE TABLE phraseTreeNodes( id INTEGER PRIMARY KEY, parentId INTEGER, wordId INTEGER, FOREIGN KEY(parentId) REFERENCES phrases(id), FOREIGN KEY(wordId) REFERENCES words(id), UNIQUE(parentId, wordId) )" )
        db.exec( "CREATE TABLE phraseTopics( phraseTreeNodeId INTEGER, topicId INTEGER, FOREIGN KEY(phraseTreeNodeId) REFERENCES phraseTreeNodes(id), FOREIGN KEY(topicId) REFERENCES topics(id), UNIQUE(phraseTreeNodeId, topicId) )" )
        //db.exec( "CREATE TABLE categories( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)" )
        db.exec( "CREATE TABLE categoriesAndContexts (topicId INTEGER, contextTopicId INTEGER, FOREIGN KEY(topicId) REFERENCES id(topics), FOREIGN KEY(contextTopicId) REFERENCES id(topics), UNIQUE(topicId, contextTopicId))" )
        //db.exec( "CREATE TABLE categoryMembership( topicId INTEGER, categoryId INTEGER, FOREIGN KEY(topicId) REFERENCES topics(id), FOREIGN KEY(categoryId) REFERENCES topics(id), UNIQUE(topicId, categoryId) )" )
        
        /*val addTopic = db.prepare( "INSERT INTO topics VALUES( NULL, ? )", HNil )
        //val addSurfaceForm = db.prepare( "INSERT INTO surfaceForms SELECT ?, id FROM topics WHERE topics.name=?" )
        val addWord = db.prepare( "INSERT OR IGNORE INTO words VALUES(NULL, ?, 0)", HNil )
        
        val getExistingTreeNode = db.prepare( "SELECT id FROM phraseTreeNodes WHERE parentId=? AND wordId=(SELECT id FROM words WHERE name=?)", Col[Int]::HNil )
        val addTreeNode = db.prepare( "INSERT INTO phraseTreeNodes VALUES( NULL, ?, (SELECT id FROM words WHERE name=?) )", HNil )
        
        val addPhraseTopic = db.prepare( "INSERT INTO phraseTopics VALUES( ?, (SELECT id FROM topics WHERE name=?) )", HNil )
        val addCategory = db.prepare( "INSERT INTO categories VALUES( NULL, ? )", HNil )
        val addCategoryMembership = db.prepare( "INSERT INTO categoryMembership VALUES( (SELECT id FROM topics WHERE name=?), (SELECT id FROM categories WHERE name=?) )", HNil )*/
        
        var count = 0
        
        db.exec( "BEGIN" )
        
        def exec( stmt : String )
        {
            db.exec( stmt )
        }
        
        def prepare[T <: HList]( query : String, row : T ) = db.prepare( query, row )
        
        def sync()
        {
            println( "Committing " + count )
            db.exec( "COMMIT" )
            db.exec( "BEGIN" )
        }
        
        def manageTransactions()
        {
            count += 1
            if ( (count % 100000) == 0 )
            {
                sync()
            }
        }
        
        def getLastInsertId() = db.getLastInsertId()
        
        def clearTopicMap()
        {
            topics.clear()
        }
        
        /*def addTopic( surfaceForm : String, topic : String )
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
                }
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
        }*/
        
        def close()
        {
            db.exec( "COMMIT" )
        }
    }
    
    private def getJobFiles( fs : FileSystem, basePath : String, directory : String ) =
    {
        val fileList = fs.listStatus( new Path( basePath + "/" + directory ) )
        
        fileList.map( _.getPath ).filter( !_.toString.endsWith( "_SUCCESS" ) )
    }
    
    def run( conf : Configuration, inputDataDirectory : String, outputFilePath : String )
    {
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        val fs = FileSystem.get(conf)   
        
        val sql = new SQLiteWriter( outputFilePath )
        
        
        val basePath = "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/" + inputDataDirectory
        
        {
            println( "Building topic index" )
            val insertTopic = sql.prepare( "INSERT INTO topics VALUES( NULL, ? )", HNil )
            
            val fileList = getJobFiles( fs, basePath, "categoriesAndContexts" )

            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                
                val topic = new Text()
                val links = new TextArrayWritable()
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( topic, links ) )
                {
                    insertTopic.exec( topic.toString )
                    sql.manageTransactions()
                }
            }
        }
        
        {
            println( "Parsing redirects" )
            val insertRedirect = sql.prepare( "INSERT OR IGNORE INTO redirects VALUES( (SELECT id FROM topics WHERE name=?), (SELECT id FROM topics WHERE name=?) )", HNil )
            
            val fileList = getJobFiles( fs, basePath, "redirects" )
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                
                val fromTopic = new Text()
                val toTopic = new Text()
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( fromTopic, toTopic ) )
                {
                    insertRedirect.exec( fromTopic.toString, toTopic.toString )
                    sql.manageTransactions()
                }
            }
            
            println( "Tidying redirects" )
            sql.sync()
            sql.exec( "DELETE FROM redirects WHERE toId IS NULL" )
            sql.exec( "DELETE FROM redirects WHERE fromId=toId" )
            // Now need to sanitise redirects. I.e. if a toTopic refers to a fromTopic elsewhere,
            // redirect again. Although experimentation suggest there may only be a few.
            
            println( "Building redirect aware topic id lookup" )
            sql.exec( "CREATE TABLE topicNameToId (name TEXT, id INTEGER, FOREIGN KEY(id) REFERENCES topic(id), UNIQUE(name))" )
            sql.exec( "INSERT INTO topicNameToId SELECT t1.name, case WHEN t2.toId IS NULL THEN t1.id ELSE t2.toId END FROM topics AS t1 LEFT JOIN redirects AS t2 ON t1.id=t2.fromId" )
            sql.sync()
        }
        
        {
            println( "Adding words" )
            val fileList = getJobFiles( fs, basePath, "wordInTopicCount" )
        
            val wordInsert = sql.prepare( "INSERT INTO words VALUES( NULL, ?, ? )", HNil )
            //id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, count INTEGER
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                val word = new Text()
                val count = new IntWritable()
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( word, count ) )
                {
                    if ( count.get > 4 )
                    {
                        wordInsert.exec( word.toString, count.get )
                        sql.manageTransactions()
                    }
                }
            }
            sql.sync()
        }
        
        {
            println( "Adding categories and contexts" )
            
            val fileList = getJobFiles( fs, basePath, "categoriesAndContexts" )
            val insertContext = sql.prepare( "INSERT OR IGNORE INTO categoriesAndContexts VALUES ((SELECT id FROM topicNameToId WHERE name=?), (SELECT id FROM topicNameToId WHERE name=?))", HNil )
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                
                val topic = new Text()
                val links = new TextArrayWritable()
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( topic, links ) )
                {
                    for ( linkTo <- links.elements )
                    {
                        insertContext.exec( topic.toString, linkTo.toString )
                        sql.manageTransactions()
                    }
                }
            }
            sql.sync()
        }
        
        // ****************************
        // TODO: MAKE THIS BIT WORK!!!!
        // ****************************
        {
            println( "Adding surface forms" )
            val fileList = getJobFiles( fs, basePath, "surfaceForms" )
        
            //val wordInsert = sql.prepare( "INSERT INTO words VALUES( NULL, ?, ? )", HNil )
            //id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, count INTEGER
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                
                val surfaceForm = new Text()
                val topics = new TextArrayWritable()
                
                
                val getWordId = sql.prepare( "SELECT id FROM words WHERE name=?", Col[Int]::HNil )
                val getPhraseTreeNodeId = sql.prepare( "SELECT id FROM phraseTreeNodes WHERE parentId=? AND wordId=?", Col[Long]::HNil )
                val addPhraseTreeNodeId = sql.prepare( "INSERT INTO phraseTreeNodes VALUES( NULL, ?, ? )", HNil )
                val addTopicToPhrase = sql.prepare( "INSERT OR IGNORE INTO phraseTopics VALUES( ?, (SELECT id FROM topicNameToId WHERE name=?) )", HNil )
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( surfaceForm, topics ) )
                {
                    var parentId = -1L
                    
                    class WordNotFoundException extends Exception
                    
                    val surfaceFormWords = Utils.luceneTextTokenizer( surfaceForm.toString )
                    try
                    {
                        // Add phrase to phrase map
                        //println( "Adding surface forms" )
                        for ( word <- surfaceFormWords )
                        {
                            getWordId.bind( word )
                            val ids = getWordId.toList
                            if ( ids == Nil )
                            {
                                //println( "Word in phrase missing from lookup: '" + word +"'" )
                                throw new WordNotFoundException()
                            }
                            else
                            {
                                assert( ids.length == 1 )
                                val wordId = _1(ids.head).get
                                //println( "Found word " + word + " " + wordId )
                                getPhraseTreeNodeId.bind( parentId, wordId )
                                val ptnIds = getPhraseTreeNodeId.toList
                                
                                if ( ptnIds != Nil )
                                {
                                    assert( ptnIds.length == 1 )
                                    parentId = _1(ptnIds.head).get
                                }
                                else
                                {
                                    addPhraseTreeNodeId.exec( parentId, wordId )
                                    parentId = sql.getLastInsertId()
                                }
                            }
                            sql.manageTransactions()
                        }
                        
                        //println( "Adding topics against phrase map" )
                        // Add all topics against phrase map terminal id
                        for ( topic <- topics.elements )
                        {
                            addTopicToPhrase.exec( parentId, topic.toString )
                            sql.manageTransactions()
                        }
                    }
                    catch
                    {
                        case e : WordNotFoundException =>
                    }
                }
            }
        }
/*
        
        {
            println( "Adding topics..." )
            val fileList = fs.listStatus( new Path( basePath + "/surfaceForms" ) )
            
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
*/
        sql.close()
        println( "*** Parse complete ***" )
    }
    
    def main( args : Array[String] )
    {
        val conf = new Configuration()
        run( conf, args(0), args(1) )
    }
}

