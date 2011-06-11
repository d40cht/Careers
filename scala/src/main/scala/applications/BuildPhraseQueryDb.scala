import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io.SequenceFile.{createWriter, Reader => HadoopReader}
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.util.{HashMap, HashSet}

import scala.collection.immutable.{TreeMap, TreeSet}

import java.io.{File, FileOutputStream, DataOutputStream, DataInputStream, FileInputStream}

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

import org.seacourt.utility._
import org.seacourt.sql.SqliteWrapper._

import org.seacourt.disambiguator.{PhraseMapLookup}

// PROFILE USING VisualVM

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
        
        db.exec( "PRAGMA cache_size=1500000" )
        //db.exec( "PRAGMA journal_mode=off" )
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
        val sql = new SQLiteWriter( outputFilePath )
        
        if (false)
        {
            conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"))
            conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"))
            val basePath = "hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/" + inputDataDirectory
            val fs = FileSystem.get(conf)
            
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
        
            // Cross-link with phraseCounts
            //val addSurfaceForm = sql.prepare( "INSERT OR IGNORE INTO surfaceForms VALUES( ?, (SELECT CAST(? AS DOUBLE)/phraseCount FROM phraseCounts WHERE phraseId=?) )", HNil )
            val addTopicToPhrase = sql.prepare( "INSERT OR IGNORE INTO phraseTopics VALUES( ?, ?, ? )", HNil )
            val lookupTopicId = sql.prepare( "SELECT id FROM topicNameToId WHERE name=?", Col[Int]::HNil )
            
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
                        var sfIdToTopicMap = TreeMap[(Int, Int), Int]()
                        
                        for ( (topic, number) <- topics )
                        {
                            //println( surfaceForm + " " + sfId + " " + topic + " " + number )
                            lookupTopicId.bind( topic )
                            
                            val results = lookupTopicId.toList
                            
                            if ( results != Nil )
                            {
                                val topicId = _1(results.head).get
                                
                                var count = number
                                val key = (sfId, topicId)
                                if ( sfIdToTopicMap.contains( key ) )
                                {
                                    count += sfIdToTopicMap( key )
                                }
                                
                                sfIdToTopicMap = sfIdToTopicMap.updated( key, count )
                            }
                        }
                        
                        for ( ((sfId, topicId), number) <- sfIdToTopicMap )
                        {
                            //println( surfaceForm + " " + sfId + " " + topicId + " " + number )
                            
                            addTopicToPhrase.exec( sfId, topicId, number )
                            sql.manageTransactions()
                        }
                        //println( surfaceForm, sfId )
                        //addSurfaceForm.exec( sfId, inTopicCount, sfId )
                    }
                }
            }
        }
        
        println( "Building indices" )
        //sql.exec( "CREATE INDEX categoryContextIndex ON categoriesAndContexts(topicId)" )
        
        // DELETE FROM categoriesAndContexts WHERE contextTopicId IS NULL
        // CREATE TABLE numTopicsForWhichThisIsAContext( topicId INTEGER, count INTEGER, UNIQUE(topicId), FOREIGN KEY(topicId) REFERENCES id(topics) )
        // PRAGMA cache_size=2000000
        // INSERT INTO numTopicsForWhichThisIsAContext SELECT contextTopicId, SUM(1) FROM categoriesAndContexts GROUP BY contextTopicId;
        
        sql.exec( "CREATE TABLE linkWeights( topicId INTEGER, contextTopicId INTEGER, weight1 DOUBLE, weight2 DOUBLE, UNIQUE(topicId, contextTopicId) )" )
        
        println( "Building bidirectional context table" )
        //CREATE TABLE bidirectionalCategoriesAndContexts( topicId INTEGER, contextTopicId INTEGER, FOREIGN KEY(topicId) REFERENCES id(topics), UNIQUE(topicId, contextTopicId) );
        //INSERT INTO bidirectionalCategoriesAndContexts SELECT contextTopicId, topicId FROM categoriesAndContexts ORDER BY contextTopicId
        //INSERT OR IGNORE INTO bidirectionalCategoriesAndContexts SELECT topicId, contextTopicId FROM categoriesAndContexts
        
        println( "Counting bidirectional contexts" )
        val numContexts = _1(sql.prepare( "SELECT COUNT(*) FROM bidirectionalCategoriesAndContexts", Col[Int]::HNil ).onlyRow).get
        
        val r = Runtime.getRuntime()
        def mem = r.totalMemory() - r.freeMemory()
        
        println( "Utilisation: " + (mem/(1024*1024) ) )
        var carr = new EfficientArray[EfficientIntPair](numContexts)
        
        if ( true )
        {
            println( "Utilisation: " + (mem/(1024*1024) ) )
            println( "Building in-memory context array of size: " + numContexts )
            var index = 0
            val eip = new EfficientIntPair(0,0)
            var biContextQuery = sql.prepare( "SELECT topicId, contextTopicId FROM categoriesAndContexts ORDER BY topicId, contextTopicId", Col[Int]::Col[Int]::HNil )
            for ( pair <- biContextQuery )
            {
                val from = _1(pair).get
                val to = _2(pair).get
                eip.first = from
                eip.second = to
                
                carr(index) = eip

                index += 1    
                if ( (index % 100000) == 0 )
                {
                    println( index + " Utilisation: " + (mem/(1024*1024) ) )
                }
            }
            carr.save( new DataOutputStream( new FileOutputStream( new File( "categories.bin" ) ) ) )
        }
        
        //carr.load( new DataInputStream( new FileInputStream( new File( "categories.bin" ) ) ) )
        
        var index = 0
        var contextQuery = sql.prepare( "SELECT topicId, contextTopicId FROM categoriesAndContexts ORDER BY topicId", Col[Int]::Col[Int]::HNil )
        def comp(x : EfficientIntPair, y : EfficientIntPair) =
        {
            if ( x.first != y.first )
            {
                x.first < y.first
            }
            else
            {
                x.second < y.second
            }
        }
        
        val insertWeightQuery = sql.prepare( "INSERT INTO linkWeight VALUES(?, ?, ?, ?)", HNil )
        for ( pair <- contextQuery )
        {
            def getContext( topicId : Int ) =
            {
                var contextSet = TreeSet[Int]()
                
                var fIndex = Utils.lowerBound( new EfficientIntPair(topicId, 0), carr, comp )
                
                var done = false
                while ( fIndex < numContexts && !done )
                {
                    val v = carr(fIndex)
                    if ( v.first == topicId )
                    {
                        contextSet = contextSet + v.second
                        fIndex += 1
                    }
                    else
                    {
                        done = true
                    }
                }
                
                contextSet
            }
            
            val fromId = _1(pair).get
            val toId = _2(pair).get
            
            val fromContext = getContext( fromId )
            val toContext = getContext( toId )
            
            val intersectionContext = fromContext & toContext
            val weight1 = intersectionContext.size.toDouble / fromContext.size.toDouble
            val weight2 = intersectionContext.size.toDouble / toContext.size.toDouble
            
            println( ":: " + fromId + ", " + toId + ", " + intersectionContext.size + ", " + fromContext.size + ", " + toContext.size )
            insertWeightQuery.exec( fromId, toId, weight1, weight2 )
            
            index += 1 
            
            if ( (index % 1000) == 0 )
            {
                println( index + " Utilisation: " + (mem/(1024*1024) ) )
            }
        }
        
        println( "Utilisation: " + (mem/(1024*1024) ) )
        
        //contextQuery = null
        
        /*val linkQuery = sql.prepare( "SELECT topicId, contextTopicId, t2.name, t3.name FROM categoriesAndContexts AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN topics AS t3 ON t1.contextTopicId=t3.id", Col[Int]::Col[Int]::Col[String]::Col[String]::HNil )
        
        val contextQuery1 = sql.prepare( "SELECT topicId FROM categoriesAndContexts WHERE contextTopicId=?", Col[Int]::HNil )
        val contextQuery2 = sql.prepare( "SELECT contextTopicId FROM categoriesAndContexts WHERE topicId=?", Col[Int]::HNil )
        
        val insertLinkQuery = sql.prepare( "INSERT INTO linkWeights VALUES(?, ?, ?, ?)", HNil )
        var count = 0
        var contextSet1 = TreeSet[Int]()
        var lastTopicId = -1
        for ( row <- linkQuery )
        {
            val topicId = _1(row).get
            val contextTopicId = _2(row).get
            val name1 = _3(row).get
            val name2 = _4(row).get
            
            if ( topicId != lastTopicId )
            {
                contextSet1 = TreeSet[Int]()
                contextQuery1.bind( topicId )
                for ( r <- contextQuery1 ) contextSet1 = contextSet1 + _1(r).get
                contextQuery2.bind( topicId )
                for ( r <- contextQuery2 ) contextSet1 = contextSet1 + _1(r).get
            }
            lastTopicId = topicId
            
            var contextSet2 = TreeSet[Int]()
            contextQuery1.bind( contextTopicId )
            for ( r <- contextQuery1 ) contextSet2 = contextSet2 + _1(r).get
            contextQuery2.bind( contextTopicId )
            for ( r <- contextQuery2 ) contextSet2 = contextSet2 + _1(r).get
            
            val intersectionSet = contextSet1 & contextSet2
            
            val intersectionSize = intersectionSet.size+1
            val set1Size = contextSet1.size
            val set2Size = contextSet2.size
            val weight1 = intersectionSize.toDouble / (1+set1Size).toDouble
            val weight2 = intersectionSize.toDouble / (1+set2Size).toDouble
            
            println( count + ": Link from " + name1 + " to " + name2 + " with weight: " + weight1 + ", " + weight2 )
            
            insertLinkQuery.exec( topicId, contextTopicId, weight1, weight2 )
            count += 1
            sql.manageTransactions()
        }*/

        sql.close()
        println( "*** Parse complete ***" )
    }
    
    def main( args : Array[String] )
    {
        val conf = new Configuration()
        run( conf, args(0), args(1) )
    }
}

