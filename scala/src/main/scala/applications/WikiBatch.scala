import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.filecache.DistributedCache

import java.io.File

import org.seacourt.sql.SqliteWrapper
import org.seacourt.mapreducejobs._
import org.seacourt.berkeleydb
import org.seacourt.utility._

import resource._

import sbt.Process._

// To add: link counts - forward and backwards.


object WikiBatch
{
    private def getJobFiles( fs : FileSystem, basePath : String, directory : String ) =
    {
        val fileList = fs.listStatus( new Path( basePath + "/" + directory ) )
        
        fileList.map( _.getPath ).filter( !_.toString.endsWith( "_SUCCESS" ) )
    }
    
    private def buildSFDb( conf : Configuration, fs : FileSystem, dbenvPath : String, basePath : String )
    {
        println( "Building surface form bdb" )
        
        for ( env <- managed( new berkeleydb.Environment( new File(dbenvPath), true ) );
              db  <- managed( env.openDb( "phrases", true ) ) )
        {
            val fileList = getJobFiles( fs, basePath, "surfaceForms" )

            for ( filePath <- fileList )
            {
                println( "Parsing: " + filePath )
                
                val surfaceForm = new Text()
                val topics = new TextArrayCountWritable()
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( surfaceForm, topics ) )
                {
                    if ( topics.elements.foldLeft(0)( _ + _._2 ) > 2 )
                    {
                        db.put( surfaceForm.toString, "" )
                    }
                }
            }
            println( "  db build complete." )
        }
    }

    def main(args:Array[String]) : Unit =
    {
        // Run Hadoop jobs
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"))
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"))
        val fs = FileSystem.get(conf)   

        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        val inputFile = args(0)
        val outputPathBase = args(1)
        val numReduces = args(2).toInt

        // TODO: An additional parse run that runs over all the topics of relevance, and a fn in Utils to
        //       specify relevance to be used in all the jobs below.

        
        
        val sfDbLocalPath = "phrasedb"
        val localTarPath = sfDbLocalPath + ".tgz"
        val remoteTarPath = outputPathBase + "/" + sfDbLocalPath + ".tgz"
        SurfaceFormsGleaner.run( "SurfaceFormsGleaner", conf, inputFile, outputPathBase + "/surfaceForms", numReduces )
        
        // Copy surface forms out into a Berkeley db and send to distributed cache
        buildSFDb(conf, fs, sfDbLocalPath, outputPathBase)
        
        // Zip up the berkeley db to a tgz
        // Copy it using DistributedCache.addCacheArchive (will unzip at the clients)
        // Make sure the flag set is: 'DistributedCache.createSymlink(Configuration)' to make it symlinked to the local FS
        // Open the berkeley db as required
        
        // Run phrasecounter so it only counts phrases that exist as surface forms
        println( "Running tar" )
        val ret : Int = ("tar czf " + localTarPath + " " + sfDbLocalPath) !
        
        require( ret == 0 )
        println( "  complete..." )
        
        println( "Copying to HDFS" )
        fs.copyFromLocalFile( false, true, new Path( localTarPath ), new Path( remoteTarPath ) )
        println( "  complete" )
        
        // Run phrasecounter so it only counts phrases that exist as surface forms
        conf.set( PhraseCounter.phraseDbRaw, sfDbLocalPath )
        conf.set( PhraseCounter.phraseDbKey, remoteTarPath )
        PhraseCounter.run( "PhraseCounter", conf, inputFile, outputPathBase + "/phraseCounts", numReduces )
        
        //WordInTopicCounter.run( "WordInTopicCounter", conf, inputFile, outputPathBase + "/wordInTopicCount", numReduces )
        //RedirectParser.run( "RedirectParser", conf, inputFile, outputPathBase + "/redirects", numReduces )
        //CategoriesAndContexts.run( "CategoriesAndContexts", conf, inputFile, outputPathBase + "/categoriesAndContexts", numReduces )
    }
}

object DatabaseBuilder
{
	def main(args:Array[String]) : Unit =
    {
        // Run Hadoop jobs
        val conf = new Configuration()

        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        val inputPathBase = args(0)

		// Now pull them all in and build the sqlite db
        PhraseMap.run( conf, inputPathBase, "testOut.sqlite3" )
	}
}

