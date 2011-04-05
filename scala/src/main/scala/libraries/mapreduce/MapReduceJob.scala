package org.seacourt.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable,IntWritable}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Job => MRJob }
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


abstract class MapReduceJob[MapKeyType <: Writable : Manifest, MapValueType <: Writable : Manifest, ReduceKeyType <: Writable : Manifest, ReduceValueType <: Writable : Manifest, OutputKeyType <: Writable : Manifest, OutputValueType <: Writable : Manifest]
{
    type MapperType = Mapper[MapKeyType, MapValueType, ReduceKeyType, ReduceValueType]
    type ReducerType = Reducer[ReduceKeyType, ReduceValueType, OutputKeyType, OutputValueType]
    type Job = MRJob
    
    def register( job : Job )
    
    def run( jobName : String, conf : Configuration, inputFileName : String, outputFilePath : String, numReduces : Int )
    {
        val job = new MRJob(conf, jobName)
        
        job.setJarByClass(classOf[MapReduceJob[_,_,_,_,_,_]])
        
        register( job )
        
        job.setNumReduceTasks( numReduces )
        
        job.setMapOutputKeyClass(manifest[ReduceKeyType].erasure)
        job.setMapOutputValueClass(manifest[ReduceValueType].erasure)
        job.setOutputKeyClass(manifest[OutputKeyType].erasure)
        job.setOutputValueClass(manifest[OutputValueType].erasure)
        
        // Output needs to be sequence file otherwise toString is called on LinkData losing information
        job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _] ])
        job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _] ])
        
        FileInputFormat.addInputPath(job, new Path(inputFileName))
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}

