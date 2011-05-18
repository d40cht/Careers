import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{Text, IntWritable}

import java.io.File

import org.seacourt.sql.SqliteWrapper
import org.seacourt.mapreducejobs._
import org.seacourt.berkeleydb
import org.seacourt.utility._

import resource._

import sbt.Process._
import scala.collection.immutable.TreeMap

// To add: link counts - forward and backwards.


object WikiBatch
{
    val wordMapName = "wordMap.bin"
    
    private def getJobFiles( fs : FileSystem, basePath : String, directory : String ) =
    {
        val fileList = fs.listStatus( new Path( basePath + "/" + directory ) )
        
        fileList.map( _.getPath ).filter( !_.toString.endsWith( "_SUCCESS" ) )
    }
    
    /*    
    var words = List[Int]()
    for ( word <- Utils.luceneTextTokenizer( sf ) )
    {
        val fl = new FixedLengthString( word )
        Utils.binarySearch( fl, wordMap, comp ) match
        {
            case Some( v ) => words = v :: words
            case _ =>
        }
    }
    */

    
    private def buildWordAndSurfaceFormsMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        println( "Building word dictionary" )
        
        {
            val fileList = getJobFiles( fs, basePath, "wordInTopicCount" )
            val builder = new EfficientArray[FixedLengthString](0).newBuilder

            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                val word = new Text()
                val count = new IntWritable()
                
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( word, count ) )
                {
                    if ( count.get() > 2 )
                    {
                        val str = word.toString()
                       
                        if ( str.length < 12 )
                        {
                            builder += new FixedLengthString( str )
                        }
                    }
                }
            }
        
            println( "Sorting array." )
            val sortedWordArray = builder.result().sortWith( _.value < _.value )
            println( "Array length: " + sortedWordArray.length )
            sortedWordArray.save( new File(wordMapName) )
        }
        
        println( "Parsing surface forms" )
        
        {
            val fileList = getJobFiles( fs, basePath, "surfaceForms" )
            val builder = new EfficientArray[FixedLengthString](0).newBuilder
    
            var wordLength = TreeMap[Int, Int]()
            var maxLength = 0
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                val surfaceForm = new Text()
                val targets = new TextArrayCountWritable()
                
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( surfaceForm, targets ) )
                {
                    val wordList = Utils.luceneTextTokenizer( surfaceForm.toString )
                    val length = wordList.length
                    
                    val count = wordLength.getOrElse( length, 0 )
                    wordLength = wordLength.updated( length, count + 1 )
                    if ( length > maxLength )
                    {
                        maxLength = length
                        println( "Max length: " + maxLength )
                    }
                }
            }
            
            for ( (length, count) <- wordLength )
            {
                println( "* " + length + " : " + count )
            }
        }
        
        
        
        println( "Copying to HDFS" )
        val remoteMapPath = basePath + "/" + wordMapName
        fs.copyFromLocalFile( false, true, new Path( wordMapName ), new Path( remoteMapPath ) )
        println( "  complete" )
        
        // Run phrasecounter so it only counts phrases that exist as surface forms
        conf.set( "wordMap", remoteMapPath )
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
        
        //WordInTopicCounter.run( "WordInTopicCounter", conf, inputFile, outputPathBase + "/wordInTopicCount", numReduces )
        //SurfaceFormsGleaner.run( "SurfaceFormsGleaner", conf, inputFile, outputPathBase + "/surfaceForms", numReduces )
        
        buildWordAndSurfaceFormsMap( conf, fs, outputPathBase )
        
        PhraseCounter.run( "PhraseCounter", conf, inputFile, outputPathBase + "/phraseCounts", numReduces )
        
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

