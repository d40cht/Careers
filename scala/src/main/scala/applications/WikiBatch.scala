import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{Text, IntWritable}

import java.io.File
import java.util.ArrayList

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
    
    class WordPhraseManager( val conf : Configuration, val fs : FileSystem, val basePath : String )
    {
        val sortedWordArray = buildWordDictionary()
        
        def buildWordDictionary() =
        {
            println( "Building word dictionary" )
            val builder = new EfficientArray[FixedLengthString](0).newBuilder
            
            val fileList = getJobFiles( fs, basePath, "wordInTopicCount" )

            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                val word = new Text()
                val count = new IntWritable()
                
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( word, count ) )
                {
                    if ( count.get() > 5 )
                    {
                        val str = word.toString()
                       
                        if ( str.getBytes("UTF-8").length < 20 )
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
            
            sortedWordArray
        }
        
        def parseSurfaceForms() = 
        {
            println( "Parsing surface forms" )
        
            val fileList = getJobFiles( fs, basePath, "surfaceForms" )
            val builder = new EfficientArray[FixedLengthString](0).newBuilder

            val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
            // (parentId : Int, wordId : Int) => thisId : Int
            var phraseData = new ArrayList[TreeMap[(Int, Int), Int]]()
            var lastId = 1
            for ( filePath <- fileList )
            {
                println( "  " + filePath )
                val surfaceForm = new Text()
                val targets = new TextArrayCountWritable()
                
                val file = new HadoopReader( fs, filePath, conf )
                while ( file.next( surfaceForm, targets ) )
                {
                    val wordList = Utils.luceneTextTokenizer( surfaceForm.toString )
                    val numWords = wordList.length
                    
                    while ( phraseData.size < numWords )
                    {
                        phraseData.add(TreeMap[(Int, Int), Int]())
                    }
                    
                    var index = 0
                    var parentId = -1
                    for ( word <- wordList )
                    {
                        val res = Utils.binarySearch( new FixedLengthString(word), sortedWordArray, comp )
                        
                        res match
                        {
                            case Some(wordId) =>
                            {
                                var layer = phraseData.get(index)
                                layer.get( (parentId, wordId) ) match
                                {
                                    case Some( elementId ) => parentId = elementId
                                    case _ =>
                                    {
                                        phraseData.set( index, layer.insert( (parentId, wordId), lastId ) )
                                        
                                        parentId = lastId
                                        lastId += 1
                                    }
                                }
                                index += 1
                            }
                            
                            case _ =>
                        }
                    }
                }
            }
            
            var idToIndexMap = new TreeMap[Int, Int]()
            
            for ( i <- 0 until phraseData.size )
            {
                println( "  Phrasedata pass: " + i )
                val treeData = phraseData.get(i)
                var newIdToIndexMap = new TreeMap[Int, Int]()
                
                // (parentId : Int, wordId : Int) => thisId : Int
                var count = 0
                val builder2 = new EfficientArray[EfficientIntPair](0).newBuilder
                for ( ((parentId, wordId), thisId) <- treeData )
                {
                    newIdToIndexMap = newIdToIndexMap.insert( thisId, count )
                    val parentArrayIndex = if (parentId == -1) -1 else idToIndexMap( parentId )
                    
                    builder2 += new EfficientIntPair( parentArrayIndex, wordId )
                    
                    count += 1
                }
                
                val sortedArray = builder2.result().sortWith( _.less(_) )
                sortedArray.save( new File( "phraseMap" + i + ".bin" ) )
                
                idToIndexMap = newIdToIndexMap
            }
        }
    }

    
    private def buildWordAndSurfaceFormsMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        val wpm = new WordPhraseManager( conf, fs, basePath )
        wpm.parseSurfaceForms() 
        
        
        // Now serialize out all the phrase data layers, re-ordering all the word ids
        
        
        
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
        
        //PhraseCounter.run( "PhraseCounter", conf, inputFile, outputPathBase + "/phraseCounts", numReduces )
        
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

