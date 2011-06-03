import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io.SequenceFile.{createWriter, Reader => HadoopReader}
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.util.{TreeMap, HashMap, HashSet}

import java.io.{File, FileOutputStream, DataOutputStream, DataInputStream, FileInputStream}

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._

import org.seacourt.disambiguator.{PhraseMapLookup}

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
    
    class SQLiteWriter( fileName : String )
    {
        //val db = new SQLiteConnection( new File(fileName) )
        val db = new SQLiteWrapper( new File( fileName ) )
        
        val topics = new HashSet[String]
        
        // Various options, inc. 2Gb page cache and no journal to massively speed up creation of this db
        
        db.exec( "PRAGMA cache_size=512000" )
        db.exec( "PRAGMA journal_mode=off" )
        db.exec( "PRAGMA synchronous=off" )

        if ( false )
        {
            db.exec( "CREATE TABLE topics( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, UNIQUE(name))" )
            db.exec( "CREATE TABLE redirects( fromId INTEGER, toId INTEGER, FOREIGN KEY(fromId) REFERENCES topics(id), FOREIGN KEY(toId) REFERENCES topics(id), UNIQUE(fromId) )" )
            db.exec( "CREATE TABLE surfaceForms( phraseTreeNodeId INTEGER, occursIn DOUBLE, UNIQUE(phraseTreeNodeId) )" )
            db.exec( "CREATE TABLE phraseTopics( phraseTreeNodeId INTEGER, topicId INTEGER, count INTEGER, FOREIGN KEY(topicId) REFERENCES topics(id), UNIQUE(phraseTreeNodeId, topicId) )" )
            db.exec( "CREATE TABLE categoriesAndContexts (topicId INTEGER, contextTopicId INTEGER, FOREIGN KEY(topicId) REFERENCES id(topics), FOREIGN KEY(contextTopicId) REFERENCES id(topics), UNIQUE(topicId, contextTopicId))" )
            
            db.exec( "CREATE TABLE phraseCounts( phraseId INTEGER, phraseCount INTEGER )" )
        }
        
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
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"))
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"))
        val fs = FileSystem.get(conf)   
        
        val basePath = "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/" + inputDataDirectory 
        
        val sql = new SQLiteWriter( outputFilePath )
        
        if (false)
        {
            {
                println( "Building topic index" )
                val topicIndexIterator = new SeqFilesIterator( conf, fs, basePath, "categoriesAndContexts", new WrappedString(), new WrappedTextArrayWritable() )
                val insertTopic = sql.prepare( "INSERT INTO topics VALUES( NULL, ? )", HNil )
                
                for ( (topic, links) <- topicIndexIterator )
                {
                    insertTopic.exec( topic )
                    sql.manageTransactions()
                }
            }

            {
                println( "Parsing redirects" )
                val insertRedirect = sql.prepare( "INSERT OR IGNORE INTO redirects VALUES( (SELECT id FROM topics WHERE name=?), (SELECT id FROM topics WHERE name=?) )", HNil )
                
                val redirectIterator = new SeqFilesIterator( conf, fs, basePath, "redirects", new WrappedString(), new WrappedString() )
                
                for ( (fromTopic, toTopic) <- redirectIterator )
                {
                    insertRedirect.exec( fromTopic, toTopic )
                    sql.manageTransactions()
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
                println( "Adding categories and contexts" )
                
                val categoryContextIterator = new SeqFilesIterator( conf, fs, basePath, "categoriesAndContexts", new WrappedString(), new WrappedTextArrayWritable() )
                
                val insertContext = sql.prepare( "INSERT OR IGNORE INTO categoriesAndContexts VALUES ((SELECT id FROM topicNameToId WHERE name=?), (SELECT id FROM topicNameToId WHERE name=?))", HNil )
                
                for ( (topic, links) <- categoryContextIterator )
                {
                    for ( linkTo <- links )
                    {
                        insertContext.exec( topic, linkTo.toString )
                        sql.manageTransactions()
                    }
                }
                
                sql.sync()
                
                println( "Building category and context counts" )
                sql.exec( "CREATE TABLE topicCountAsContext(topicId INTEGER, count INTEGER)" )
                sql.exec( "INSERT INTO topicCountAsContext SELECT contextTopicId, sum(1) FROM categoriesAndContexts GROUP BY contextTopicId" )
                sql.exec( "CREATE INDEX topicCountAsContextIndex ON topicCountAsContext(topicId)" )
                sql.sync()
            }

            {
                val insertPhraseCount = sql.prepare( "INSERT INTO phraseCounts VALUES(?, ?)", HNil )
                val phraseCountIterator = new SeqFilesIterator( conf, fs, basePath, "phraseCounts", new WrappedInt(), new WrappedInt() )
                for ( (phraseId, numTopicsPhraseFoundIn) <- phraseCountIterator )
                {
                    // Build a sqlite dictionary for this
                    insertPhraseCount.exec( phraseId, numTopicsPhraseFoundIn )
                    sql.manageTransactions()
                }
            }
            sql.exec( "CREATE INDEX phraseCountIndex ON phraseCounts(phraseId)" )
            sql.sync()
        }

        
        // Cross-link with phraseCounts
        //val addSurfaceForm = sql.prepare( "INSERT OR IGNORE INTO surfaceForms VALUES( ?, (SELECT CAST(? AS DOUBLE)/phraseCount FROM phraseCounts WHERE phraseId=?) )", HNil )
        val addTopicToPhrase = sql.prepare( "INSERT INTO phraseTopics VALUES( ?, (SELECT id FROM topicNameToId WHERE name=?), ? )", HNil )
        
        // :: condoleezza rice: count should be:  Main:Condoleezza Rice, 1032

        {
            val pml = new PhraseMapLookup()
            pml.load( new DataInputStream( new FileInputStream( new File( "phraseMap.bin" ) ) ) )
            
            val surfaceFormIterator = new SeqFilesIterator( conf, fs, basePath, "surfaceForms", new WrappedString(), new WrappedTextArrayCountWritable() )
            for ( (surfaceForm, topics) <- surfaceFormIterator )
            {
                // Get the id of the surface form
                val sfId = pml.getIter().find( surfaceForm )
                
                if ( sfId != -1 )
                {
                    var inTopicCount = 0
                    for ( (topic, number) <- topics )
                    {
                        // TODO: Take number of times this surface form points to this target into account.
                        addTopicToPhrase.exec( sfId, topic.toString, number )
                        sql.manageTransactions()
                        inTopicCount += number
                    }
                    
                    //println( surfaceForm, sfId )
                    //addSurfaceForm.exec( sfId, inTopicCount, sfId )
                }
            }
        }
        
        println( "Building indices" )
        //sql.exec( "CREATE INDEX categoryContextIndex ON categoriesAndContexts(topicId)" )

        sql.close()
        println( "*** Parse complete ***" )
    }
    
    def main( args : Array[String] )
    {
        val conf = new Configuration()
        run( conf, args(0), args(1) )
    }
}

