package org.seacourt.mapreducejobs

import org.apache.hadoop.io.Text

import org.dbpedia.extraction.wikiparser._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeSet

import org.seacourt.mapreduce._
import org.seacourt.utility._

// TODO:
// Sanity check text to remove junk

object SurfaceFormsGleaner extends MapReduceJob[Text, Text, Text, Text, Text, TextArrayWritable]
{
    class JobMapper extends MapperType
    {
        def mapWork( topicTitle : String, topicText : String, output : (String, String) => Unit )
        {
            try
            {
                val parsed = Utils.wikiParse( topicTitle, topicText )
                
                var resSet = new TreeSet[(String, String)]
                Utils.traverseWikiTree( parsed, element =>
                {
                    element match
                    {
                        case InternalLinkNode( destination, children, line ) =>
                        {
                            if ( children.length != 0 &&
                                (destination.namespace.toString() == "Main" /*||
                                 destination.namespace.toString() == "Category"*/) )
                            {
                                children(0) match
                                {
                                    case TextNode( surfaceForm, line ) =>
                                    {
                                        val sf = Utils.normalize( surfaceForm )
                                        val dest = Utils.normalizeLink( destination )
                                        val newSF = (sf, dest)
                                        resSet = resSet + newSF
                                    }
                                    case _ =>
                                }
                            }
                        }
                        case _ =>
                    }
                } )
                
                output( Utils.normalize( topicTitle ), Utils.normalizeLink( topicTitle ) )
                for ( (sf, dest) <- resSet )
                {
                    output( sf, dest )
                }
            }
            catch
            {
                case e : WikiParserException =>
                case _ => 
            }
        }
        
        override def map( topicTitle : Text, topicText : Text, output : MapperType#Context )
        {
            mapWork( topicTitle.toString, topicText.toString, (key, value) => output.write( new Text(key), new Text(value) ) )
        }
    }
     
    class JobReducer extends ReducerType
    {
        override def reduce( surfaceForm : Text, targets: java.lang.Iterable[Text], output : ReducerType#Context )
        {
            // TODO: COUNT THE NUMBER OF TOPICS FOR EACH SURFACE FORM -> TOPIC PAIR
            var targetCounts = new TreeMap[Text, Int]
            for ( target <- targets )
            {
                var currCount = 0
                if ( targetCounts.contains( target ) )
                {
                    currCount = targetCounts(target)
                }
                
                targetCounts = targetCounts.updated( target, currCount + 1 )
            }
            
            if ( targetCounts.size < 1000 )
            {
                val outValues = new TextArrayWritable()
                for ( (target, count) <- targetCounts )
                {
                    outValues.append( target.toString() )
                }
                output.write( surfaceForm, outValues )
            }
            
            
            /*val seen = new HashSet[Text]
            val outValues = new TextArrayWritable()
            var count = 0
            for ( target <- targets )
            {
                if ( !seen.contains(value) )
                {
                    outValues.append( value.toString() )
                    count += 1
                }
                if ( count > 1000 ) return Unit
            }
            output.write( surfaceForm, outValues )*/
        }
    }
    
    override def register( job : Job )
    {
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
    }
}


