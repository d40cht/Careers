package org.seacourt.wikibatch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopReader}
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{Writable, Text, IntWritable}

import java.io.{File, DataOutputStream, DataInputStream, FileOutputStream, FileInputStream}
import java.util.ArrayList

import org.seacourt.sql.SqliteWrapper
import org.seacourt.mapreducejobs._
import org.seacourt.berkeleydb
import org.seacourt.utility._
import org.seacourt.disambiguator.{PhraseMapBuilder, PhraseMapLookup}

import resource._

import sbt.Process._
import scala.collection.immutable.TreeMap


object WikiBatch
{
    val phraseMapFileName = "phraseMap.bin"
    
    private def buildWordAndSurfaceFormsMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        val wordSource = new SeqFilesIterator( conf, fs, basePath, "wordInTopicCount", new WrappedString(), new WrappedInt() )
        val wpm = new PhraseMapBuilder( "lookup/wordMap", "lookup/phraseMap" )
        val wordMap = wpm.buildWordMap( wordSource )
        
        val sfSource = new SeqFilesIterator( conf, fs, basePath, "surfaceForms", new WrappedString(), new WrappedTextArrayCountWritable() )
        val phraseMap = wpm.parseSurfaceForms( sfSource )
        
        val pml = new PhraseMapLookup( wordMap, phraseMap )
        
        pml.save( new DataOutputStream( new FileOutputStream( new File( phraseMapFileName ) ) ) )
        
        
        // Now copy the lookup over to HDFS and hence to the distributed cache
        
        println( "Copying to HDFS" )
        val remoteMapPath = basePath + "/" + phraseMapFileName
        fs.copyFromLocalFile( false, true, new Path( phraseMapFileName ), new Path( remoteMapPath ) )
        println( "  complete" )
    }
    
    def validatePhraseMap( conf : Configuration, fs : FileSystem, basePath : String )
    {
        println( "Validating surface forms" )
        
        val lookup = new PhraseMapLookup()
        lookup.load( new DataInputStream( new FileInputStream( new File( phraseMapFileName ) ) ) )
        val sfSource2 = new SeqFilesIterator( conf, fs, basePath, "surfaceForms", new WrappedString(), new WrappedTextArrayCountWritable() )
        
        var count = 0
        var countFound = 0
        for ( (surfaceForm, targets) <- sfSource2 )
        {
            if ( lookup.getIter().find( surfaceForm ) != -1 ) countFound += 1
            count += 1
        }
        println( "Found " + countFound + ", total: " + count )
    }
   

    def main(args:Array[String]) : Unit =
    {
        // Run Hadoop jobs
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"))
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"))
        val fs = FileSystem.get(conf)

        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        val inputFile = otherArgs(0)
        val outputPathBase = otherArgs(1)
        val numReduces = otherArgs(2).toInt
        
        conf.set( "mapred.compress.map.output", "true" )
        conf.set( "org.seacourt.phrasemap", outputPathBase + "/" + phraseMapFileName )

        // TODO: An additional parse run that runs over all the topics of relevance, and a fn in Utils to
        //       specify relevance to be used in all the jobs below.
        
        WordInTopicCounter.run( "WordInTopicCounter", conf, inputFile, outputPathBase + "/wordInTopicCount", numReduces )
        SurfaceFormsGleaner.run( "SurfaceFormsGleaner", conf, inputFile, outputPathBase + "/surfaceForms", numReduces )
        
        buildWordAndSurfaceFormsMap( conf, fs, outputPathBase )
        validatePhraseMap( conf, fs, outputPathBase )
        
        PhraseCounter.run( "PhraseCounter", conf, inputFile, outputPathBase + "/phraseCounts", numReduces )
        
        RedirectParser.run( "RedirectParser", conf, inputFile, outputPathBase + "/redirects", numReduces )
        CategoriesAndContexts.run( "CategoriesAndContexts", conf, inputFile, outputPathBase + "/categoriesAndContexts", numReduces )
    }
}

