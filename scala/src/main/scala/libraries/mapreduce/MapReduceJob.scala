package org.seacourt.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable,IntWritable}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._

import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.wikiparser.{Node}

import scala.collection.immutable.TreeSet
import java.io.{File, BufferedReader, FileReader, StringReader, Reader}
import java.io.{DataInput, DataOutput}

import org.seacourt.utility.Utils._


abstract class MapReduceJob[MapKeyType : Manifest, MapValueType : Manifest, ReduceKeyType : Manifest, ReduceValueType : Manifest, OutputKeyType : Manifest, OutputValueType : Manifest]
{
    
    def mapfn( key : MapKeyType, value : MapValueType, outputFn : (ReduceKeyType, ReduceValueType) => Unit )
    def reducefn( key : ReduceKeyType, values : java.lang.Iterable[ReduceValueType], outputFn : (OutputKeyType, OutputValueType) => Unit )
    
    class JobMapper extends Mapper[MapKeyType, MapValueType, ReduceKeyType, ReduceValueType]
    {
        type contextType = Mapper[MapKeyType, MapValueType, ReduceKeyType, ReduceValueType]#Context
        override def map( key : MapKeyType, value : MapValueType, context : contextType )
        {
            mapfn( key, value, (k : ReduceKeyType, v : ReduceValueType) => context.write(k, v) )
        }
    }
    
    class JobReducer extends Reducer[ReduceKeyType, ReduceValueType, OutputKeyType, OutputValueType]
    {
        type contextType = Reducer[ReduceKeyType, ReduceValueType, OutputKeyType, OutputValueType]#Context
        override def reduce( key : ReduceKeyType, values : java.lang.Iterable[ReduceValueType], context : contextType )
        {
            reducefn( key, values, (k : OutputKeyType, v : OutputValueType) => context.write(k, v) )
        }
    }
    
    def run( jobName : String, conf : Configuration, inputFileName : String, outputFilePath : String, numReduces : Int )
    {
        val job = new Job(conf, jobName)
        
        job.setJarByClass(classOf[JobMapper])
        job.setMapperClass(classOf[JobMapper])
        job.setReducerClass(classOf[JobReducer])
        job.setNumReduceTasks( numReduces )
        
        job.setInputFormatClass(classOf[SequenceFileInputFormat[MapKeyType, MapValueType] ])
        
        job.setMapOutputKeyClass(manifest[ReduceKeyType].erasure)
        job.setMapOutputValueClass(manifest[ReduceValueType].erasure)
        job.setOutputKeyClass(manifest[OutputKeyType].erasure)
        job.setOutputValueClass(manifest[OutputValueType].erasure)
        
        // Output needs to be sequence file otherwise toString is called on LinkData losing information
        job.setOutputFormatClass(classOf[SequenceFileOutputFormat[OutputKeyType, OutputValueType] ])
        
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}

