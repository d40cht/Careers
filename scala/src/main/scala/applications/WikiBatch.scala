package org.seacourt.wikibatch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{Writable, Text, IntWritable}

import java.io.{File, DataOutputStream, DataInputStream, FileOutputStream, FileInputStream}
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
    class PhraseMapLookup( val wordMap : EfficientArray[FixedLengthString], val phraseMap : ArrayList[EfficientArray[EfficientIntPair]] )
    {
        def this() = this( new EfficientArray[FixedLengthString](0), new ArrayList[EfficientArray[EfficientIntPair]]() )
        
        // sortedWordArray.save( new File(wordMapBase + ".bin") )
        // phraseMap.result().save( new File( phraseMapBase + i + ".bin" ) )
        
        def save( outStream : DataOutputStream )
        {
            wordMap.save( outStream )
            outStream.writeInt( phraseMap.size )
            for ( i <- 0 until phraseMap.size )
            {
                phraseMap.get(i).save( outStream )
            }
        }
        
        def load( inStream : DataInputStream )
        {
            wordMap.clear()
            wordMap.load( inStream )
            val depth = inStream.readInt()
            phraseMap.clear()
            for ( i <- 0 until depth )
            {
                val level = new EfficientArray[EfficientIntPair](0)
                level.load( inStream )
                phraseMap.add( level )
            }
        }
    }
    
    
    class PhraseMapReader( val pml : PhraseMapLookup )
    {
        def phraseByIndex( index : Int ) : List[String] =
        {
            var level = 0
            var found = false
            var iter = index
            while (!found)
            {
                val length = pml.phraseMap.get(level).length
                if ( iter >= length )
                {
                    iter -= length
                    level += 1
                }
                else
                {
                    found = true
                }
            }
            
            
            var res = List[String]()
            while (level != -1)
            {
                val el = pml.phraseMap.get(level)( iter )
                
                res = (pml.wordMap(el.second).value) :: res
                iter = el.first
                level -= 1
            }
            
            res
        }
        
        def find( phrase : String ) : Int =
        {
            val wordList = Utils.luceneTextTokenizer( phrase )
            
            val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
            
            var wordIds = wordList.map( (x: String) => Utils.binarySearch( new FixedLengthString(x), pml.wordMap, comp ) )
            
            //println( phrase + ": " + wordIds )
            
            var foundIndex = 0
            var parentId = -1
            var i = 0

            while ( (wordIds != Nil) )
            {
                if ( i >= pml.phraseMap.size)
                {
                    return -1
                }
                
                val wordId = wordIds.head
                val thisLevel = pml.phraseMap.get(i)
                wordId match
                {
                    case Some( wordIndex ) =>
                    {
                        val pm = Utils.binarySearch( new EfficientIntPair( parentId, wordIndex ), thisLevel, (x:EfficientIntPair, y:EfficientIntPair) => x.less(y) )
                        //println( i + " >- " + parentId + " " + wordIndex + " " + pm )
                        pm match
                        {
                            case Some( levelIndex ) =>
                            {
                                parentId = levelIndex
                            }
                            case _ => return -1
                        }
                    }
                    case _ => return -1
                }
                
                wordIds = wordIds.tail
                if ( wordIds == Nil )
                {
                    foundIndex += parentId
                }
                else
                {
                    foundIndex += thisLevel.length
                }
            
                i += 1   
            }
            
            return foundIndex
        }
    }
    
    
    
    class PhraseMapBuilder( wordMapBase : String, phraseMapBase : String )
    {
        val pml = new PhraseMapLookup()
        
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
            sortedWordArray.save( new DataOutputStream( new FileOutputStream( new File(wordMapBase + ".bin") ) ) )
            
            sortedWordArray
        }
        
        def parseSurfaceForms( sfSource : KVWritableIterator[Text, TextArrayCountWritable] ) = 
        {
            println( "Parsing surface forms" )
            
            val wordMap = new EfficientArray[FixedLengthString](0)
            wordMap.load( new DataInputStream( new FileInputStream( new File(wordMapBase + ".bin") ) ) )
            
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
            
            var arrayData = new ArrayList[EfficientArray[EfficientIntPair]]()
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
                
                arrayData.add( builder2.result() )
                idToIndexMap = newIdToIndexMap
                
                phraseData.set( i, null )
            }
            
            arrayData
        }
    }

    
    private def buildWordAndSurfaceFormsMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        val wordSource = new SeqFilesIterator[Text, IntWritable]( conf, fs, basePath, "wordInTopicCount" )
        val wpm = new PhraseMapBuilder( "lookup/wordMap", "lookup/phraseMap" )
        val wordMap = wpm.buildWordMap( wordSource )
        
        val sfSource = new SeqFilesIterator[Text, TextArrayCountWritable]( conf, fs, basePath, "surfaceForms" )
        val phraseMap = wpm.parseSurfaceForms( sfSource )
        
        val pml = new PhraseMapLookup( wordMap, phraseMap )
        
        
        // Then validate
        println( "Validating surface forms" )
        
        {
            val rb = new WikiBatch.PhraseMapReader( pml )
            
            val sfSource2 = new SeqFilesIterator[Text, TextArrayCountWritable]( conf, fs, basePath, "surfaceForms" )
            
            val surfaceForm = new Text()
            val targets = new TextArrayCountWritable()
            var count = 0
            var countFound = 0
            while ( sfSource2.getNext( surfaceForm, targets ) )
            {
                if ( rb.find( surfaceForm.toString ) != -1 ) countFound += 1
                count += 1
            }
            println( "Found " + countFound + ", total: " + count )
        }
        
        pml.save( new DataOutputStream( new FileOutputStream( new File( "phraseMap.bin" ) ) ) )
        
        // Now copy the lookup over to HDFS and hence to the distributed cache
        
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

