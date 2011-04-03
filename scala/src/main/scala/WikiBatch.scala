import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

import java.io.File
import SqliteWrapper._
import com.almworks.sqlite4java._

// Run: Category membership, Surface forms, Word in document count.

// To add: link counts - forward and backwards.
// Additionally include contexts ala microsoft research paper.


object WikiBatch
{
    def main(args:Array[String]) : Unit =
    {
        // Run Hadoop jobs
        val conf = new Configuration()

        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        val inputFile = args(0)
        val outputPathBase = args(1)
        val numReduces = args(2).toInt

        // Refactor the following to:
        // WordInDocumentMembership (for tf-idf)
        // A utility fn that parses topic names and decides whether to process them (including for links)
        // A class that processes each topic and produces a mapping from topicName -> (topicId, isRedirect, isDisambig)
        // A class that processes all links (inc. redirects): parentTopic -> destTopic, surface form, isInFirstParagraph, isRedirect

        // TODO: An additional parse run that runs over all the topics of relevance, and a fn in Utils to
        //       specify relevance to be used in all the jobs below.

        WordInTopicCounter.run( "WordInTopicCounter", conf, inputFile, outputPathBase + "/wordInTopicCount", numReduces )
        SurfaceFormsGleaner.run( "SurfaceFormsGleaner", conf, inputFile, outputPathBase + "/surfaceForms", numReduces )
        RedirectParser.run( "RedirectParser", conf, inputFile, outputPathBase + "/redirects", numReduces )
        //SurfaceForms.run( conf, inputFile, outputPathBase + "/surfaceForms", numReduces )
        //ComprehensiveLinkParser.run( conf, inputFile, outputPathBase + "/links", numReduces )
   
        //CategoryMembership.run( conf, inputFile, outputPathBase + "/categoryMembership", numReduces )
        //RedirectParser.run( conf, inputFile, outputPathBase + "/redirects", numReduces)
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
