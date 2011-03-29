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
        println( "Here1" )
        val blah = new SQLiteWrapper( new File( "testook.sqlite" ) )
        println( "Here2" )
        
        val conf = new Configuration()

        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        val inputFile = args(0)
        val outputPathBase = args(1)
        val outputDbFileName = args(2)
        val numReduces = args(3).toInt

        if (false)
        {            
            CategoryMembership.run( conf, inputFile, outputPathBase + "/categoryMembership", numReduces )
            SurfaceForms.run( conf, inputFile, outputPathBase + "/surfaceforms", numReduces)
            WordInDocumentMembership.run( conf, inputFile, outputPathBase + "/wordInDocumentCount", numReduces)
        }
        
        // Now pull them all in and build the sqlite db
        PhraseMap.run( conf, outputPathBase, "testOut.sqlite3" )
    }
}
