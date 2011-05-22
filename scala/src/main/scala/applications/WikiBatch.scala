package org.seacourt.wikibatch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{Writable, Text, IntWritable}

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

trait KVWritableIterator[KeyType, ValueType]
{
    def getNext( key : KeyType, value : ValueType ) : Boolean
}

class SeqFilesIterator[KeyType <: Writable, ValueType <: Writable]( val conf : Configuration, val fs : FileSystem, val basePath : String, val seqFileName : String ) extends KVWritableIterator[KeyType, ValueType]
{
    var fileList = getJobFiles( fs, basePath, seqFileName )
    var currFile = advanceFile()
    
    private def getJobFiles( fs : FileSystem, basePath : String, directory : String ) =
    {
        val fileList = fs.listStatus( new Path( basePath + "/" + directory ) ).toList
        
        fileList.map( _.getPath ).filter( !_.toString.endsWith( "_SUCCESS" ) )
    }
    
    private def advanceFile() : HadoopReader =
    {
        if ( fileList == Nil )
        {
            null
        }
        else
        {
            println( "Reading file: " + fileList.head )
            val reader = new HadoopReader( fs, fileList.head, conf )
            fileList = fileList.tail
            reader
        }
    }
    
    override def getNext( key : KeyType, value : ValueType ) : Boolean =
    {
        var success = currFile.next( key, value )
        if ( !success )
        {
            currFile = advanceFile()
            
            if ( currFile != null )
            {
                success = currFile.next( key, value )
            }
        }
        success
    }
}


object WikiBatch
{
    class PhraseMapReader( wordMapBase : String, phraseMapBase : String, maxPhraseLength : Int )
    {
        val wordMap = new EfficientArray[FixedLengthString](0)
        wordMap.load( new File(wordMapBase + ".bin") )
        
        val phraseMap = new ArrayList[EfficientArray[EfficientIntPair]]()
        for ( i <- 0 until maxPhraseLength )
        {
            val phraseLevel = new EfficientArray[EfficientIntPair](0)
            phraseLevel.load( new File(phraseMapBase + i + ".bin") )
            phraseMap.add( phraseLevel )
        }
        
        def find( phrase : String ) : Boolean =
        {
            val wordList = Utils.luceneTextTokenizer( phrase )
            
            val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
            
            val wordIds = wordList.map( (x: String) => Utils.binarySearch( new FixedLengthString(x), wordMap, comp ) )
            
            println( phrase + ": " + wordIds )
            
            var parentId = -1
            var i = 0
            for ( wordId <- wordIds )
            {
                wordId match
                {
                    case Some( wordIndex ) =>
                    {
                        val pm = Utils.binarySearch( new EfficientIntPair( parentId, wordIndex ), phraseMap.get(i), (x:EfficientIntPair, y:EfficientIntPair) => x.less(y) )
                        println( i + " >- " + parentId + " " + wordIndex + " " + pm )
                        pm match
                        {
                            case Some( levelIndex ) =>
                            {
                                parentId = levelIndex
                            }
                            case _ => return false
                        }
                    }
                    case _ => return false
                }
                
                i += 1
            }
            
            
            return true
        }
    }
    
    class PhraseMapBuilder( wordMapBase : String, phraseMapBase : String )
    {
        def buildWordMap( wordSource : KVWritableIterator[Text, IntWritable] ) =
        {
            println( "Building word dictionary" )
            val builder = new EfficientArray[FixedLengthString](0).newBuilder
            
            val word = new Text()
            val count = new IntWritable()
            while ( wordSource.getNext( word, count ) )
            {
                if ( count.get() > 4 )
                {
                    val str = word.toString()
                   
                    if ( str.getBytes("UTF-8").length < 20 )
                    {
                        builder += new FixedLengthString( str )
                    }
                }
            }
        
            println( "Sorting array." )
            val sortedWordArray = builder.result().sortWith( _.value < _.value )
            
            var index = 0
            for ( word <- sortedWordArray )
            {
                //println( " >> -- " + word.value + ": " + index )
                index += 1
            }
            println( "Array length: " + sortedWordArray.length )
            sortedWordArray.save( new File(wordMapBase + ".bin") )
            
            sortedWordArray
        }
        
        def parseSurfaceForms( sfSource : KVWritableIterator[Text, TextArrayCountWritable] ) = 
        {
            println( "Parsing surface forms" )
            
            val wordMap = new EfficientArray[FixedLengthString](0)
            wordMap.load( new File(wordMapBase + ".bin") )
            
            val builder = new EfficientArray[FixedLengthString](0).newBuilder

            val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
            // (parentId : Int, wordId : Int) => thisId : Int
            var phraseData = new ArrayList[TreeMap[(Int, Int), Int]]()
            var lastId = 1
            
            val surfaceForm = new Text()
            val targets = new TextArrayCountWritable()
            while ( sfSource.getNext( surfaceForm, targets ) )
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
                    val res = Utils.binarySearch( new FixedLengthString(word), wordMap, comp )
                    
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
            
            var idToIndexMap = new TreeMap[Int, Int]()
            
            for ( i <- 0 until phraseData.size )
            {
                println( "  Phrasedata pass: " + i )
                val treeData = phraseData.get(i)
                var newIdToIndexMap = new TreeMap[Int, Int]()
                
                // (parentId : Int, wordId : Int) => thisId : Int
                
                var tempMap = new TreeMap[(Int, Int), Int]()
                for ( ((parentId, wordId), thisId) <- treeData )
                {
                    val parentArrayIndex = if (parentId == -1) -1 else idToIndexMap( parentId )
                    tempMap = tempMap.insert( (parentArrayIndex, wordId), thisId )
                }
                
                val builder2 = new EfficientArray[EfficientIntPair](0).newBuilder
                var count = 0
                for ( ((parentArrayIndex, wordId), thisId) <- tempMap )
                {
                    newIdToIndexMap = newIdToIndexMap.insert( thisId, count )
                    builder2 += new EfficientIntPair( parentArrayIndex, wordId )
                    
                    //println( i + "**)) -- " + count + " - " + parentArrayIndex + " -> " + wordId )
                    count += 1
                }
                
                /*var count = 0
                val builder2 = new EfficientArray[EfficientIntPair](0).newBuilder
                for ( ((parentId, wordId), thisId) <- treeData )
                {
                    newIdToIndexMap = newIdToIndexMap.insert( thisId, count )
                    val parentArrayIndex = if (parentId == -1) -1 else idToIndexMap( parentId )
                    
                    builder2 += new EfficientIntPair( parentArrayIndex, wordId )
                    
                    count += 1
                }
                
                val sortedArray = builder2.result().sortWith( _.less(_) )
                sortedArray.save( new File( phraseMapBase + i + ".bin" ) )*/
                builder2.result().save( new File( phraseMapBase + i + ".bin" ) )
                
                idToIndexMap = newIdToIndexMap
                
                phraseData.set( i, null )
            }
            
            phraseData.size
        }
    }

    
    private def buildWordAndSurfaceFormsMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        val wordSource = new SeqFilesIterator[Text, IntWritable]( conf, fs, basePath, "wordInTopicCount" )
        val wpm = new PhraseMapBuilder( "lookup/wordMap", "lookup/phraseMap" )
        wpm.buildWordMap( wordSource )
        
        val sfSource = new SeqFilesIterator[Text, TextArrayCountWritable]( conf, fs, basePath, "surfaceForms" )
        wpm.parseSurfaceForms( sfSource )
        
        
        // Now serialize out all the phrase data layers, re-ordering all the word ids
        
        /*println( "Copying to HDFS" )
        val remoteMapPath = basePath + "/" + wordMapName
        fs.copyFromLocalFile( false, true, new Path( wordMapName ), new Path( remoteMapPath ) )
        println( "  complete" )
        
        // Run phrasecounter so it only counts phrases that exist as surface forms
        conf.set( "wordMap", remoteMapPath )*/
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

/*object DatabaseBuilder
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
}*/

