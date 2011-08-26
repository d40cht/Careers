package org.seacourt.disambiguator

import org.seacourt.utility._
import java.io.{File, DataOutputStream, DataInputStream, FileOutputStream, FileInputStream}
import java.util.ArrayList
import scala.collection.immutable.TreeMap

import scala.collection.Iterator

import com.weiglewilczek.slf4s.{Logging}

class PhraseMapLookup( val wordMap : EfficientArray[FixedLengthString], val phraseMap : ArrayList[EfficientArray[EfficientIntPair]] ) extends Logging
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
    
    def dump()
    {
        var i = 0
        for ( el <- wordMap )
        {
            println( i + " --> " + el.value )
            i += 1
        }
        
        for ( k <- 0 until phraseMap.size )
        {
            val el = phraseMap.get(k)
            var j = 0
            for ( el2 <- el )
            {
                println( k + ", " + j + " --> " + el2.first + " " + el2.second )
                j += 1
            }
        }
    }
    
    def phraseByIndex( index : Int ) : List[String] =
    {
        var level = 0
        var found = false
        var iter = index
        while (!found)
        {
            val length = phraseMap.get(level).length
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
            val el = phraseMap.get(level)( iter )
            
            res = (wordMap(el.second).value) :: res
            iter = el.first
            level -= 1
        }
        
        res
    }
    
    
    private val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
    
    def lookupWord( word : String ) = Utils.binarySearch( new FixedLengthString(word), wordMap, comp )
    
    def getIter() = new PhraseMapIter()
    
    class PhraseMapIter()
    {
        var level = 0
        var parentId = -1
        var foundIndex = 0
        
        def update( wordIndex : Int ) : Int =
        {
            if ( level >= phraseMap.size )
            {
                return -1
            }
            
            val thisLevel = phraseMap.get(level)

            val pm = Utils.binarySearch( new EfficientIntPair( parentId, wordIndex ), thisLevel, (x:EfficientIntPair, y:EfficientIntPair) => x.less(y) )
            pm match
            {
                case Some( levelIndex ) =>
                {
                    parentId = levelIndex
                }
                case _ => return -1
            }
            
            val currPhraseIndex = foundIndex + parentId
            
            foundIndex += thisLevel.length
        
            level += 1  
            
            currPhraseIndex
        }
        
        def find( phrase : String ) : Int =
        {
            val wordList = Utils.luceneTextTokenizer( phrase )

            var wordIds = wordList.map( (x: String) => Utils.binarySearch( new FixedLengthString(x), wordMap, comp ) )
                        
            var foundIndex = 0
            var parentId = -1
            var i = 0

            var res = 0
            for ( wordId <- wordIds )
            {
                wordId match
                {
                    case Some( wordIndex ) =>
                    {
                        res = update( wordIndex )
                        
                        if ( res == -1 )
                        {
                            return -1
                        }
                    }
                    case _ => return -1
                }
                
                wordIds = wordIds.tail
            }
            
            return res
        }
    }
}


class PhraseMapBuilder( wordMapBase : File, phraseMapBase : File ) extends Logging
{
    val pml = new PhraseMapLookup()
    
    //def buildWordMap( wordSource : KVWritableIterator[Text, IntWritable] ) =
    def buildWordMap( wordSource : Iterator[(String, Int)] ) =
    {
        logger.debug( "Building word dictionary" )
        val builder = new EfficientArray[FixedLengthString](0).newBuilder
        
        for ( (word, count) <- wordSource )
        {
            if ( count > 4 )
            {
                if ( word.getBytes("UTF-8").length < 20 )
                {
                    builder += new FixedLengthString( word )
                }
            }
        }
    
        logger.debug( "Sorting array." )
        val sortedWordArray = builder.result().sortWith( _.value < _.value )
        
        var index = 0
        for ( word <- sortedWordArray )
        {
            //println( " >> -- " + word.value + ": " + index )
            index += 1
        }
        logger.debug( "Array length: " + sortedWordArray.length )
        sortedWordArray.save( new DataOutputStream( new FileOutputStream( wordMapBase + ".bin" ) ) )
        
        sortedWordArray
    }
    
    //def parseSurfaceForms( sfSource : KVWritableIterator[Text, TextArrayCountWritable] ) =
    def parseSurfaceForms( sfSource : Iterator[(String, List[(String, Int)])] ) =  
    {
        logger.debug( "Parsing surface forms" )
        
        val wordMap = new EfficientArray[FixedLengthString](0)
        wordMap.load( new DataInputStream( new FileInputStream( wordMapBase + ".bin" ) ) )
        
        val builder = new EfficientArray[FixedLengthString](0).newBuilder

        val comp = (x : FixedLengthString, y : FixedLengthString) => x.value < y.value
        // (parentId : Int, wordId : Int) => thisId : Int
        var phraseData = new ArrayList[TreeMap[(Int, Int), Int]]()
        var lastId = 1
        
        /*val surfaceForm = new Text()
        val targets = new TextArrayCountWritable()
        while ( sfSource.getNext( surfaceForm, targets ) )
        {*/
        for ( (surfaceForm, targets) <- sfSource )
        {
            val wordList = Utils.luceneTextTokenizer( surfaceForm )
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
            logger.debug( "  Phrasedata pass: " + i )
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

