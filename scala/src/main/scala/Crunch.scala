import java.io.{FileInputStream, BufferedInputStream}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.io.SequenceFile.{createWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK

import scala.io.Source
import scala.xml.pull.{XMLEventReader, EvElemStart, EvElemEnd, EvText}



object Crunch
{
    private def parsePage( parser : XMLEventReader ) : (String, Long, Long, String) =
    {
        var title = ""
        var id = 0
        var revision = 0
        var text = ""
        var done = false
        while ( parser.hasNext && !done )
        {
            parser.next match
            {
                case EvElemStart(_, "title", _, _ ) =>
                {
                    title = getText( parser, "title" )
                }
                /*case EvElemStart(_, "revision", _, _) =>
                {
                    // Need to get the 'id' from revision
                    revision = getText( parser, "revision" ).toInt
                }*/
                case EvElemStart(_, "id", _, _ ) =>
                {
                    id = getText( parser, "id" ).toInt
                }
                case EvElemStart(_, "text", _, _ ) =>
                {
                    text = getText( parser, "text" )
                }
                case EvElemEnd(_, "page") =>
                {
                    done = true
                }
                case _ =>
            }
        }
        return (title, id, revision, text)
    }

    private def getText( parser : XMLEventReader, inTag : String ) : String =
    {
        var fullText = ""
        var done = false
        while ( parser.hasNext && !done )
        {
            parser.next match
            {
                case EvElemEnd(_, tagName ) =>
                {
                    assert( tagName.equalsIgnoreCase(inTag) )
                    done = true
                }
                case EvText( text ) =>
                {
                    fullText += text
                }
                case _ =>
            }
        }
        return fullText
    }
    def main( args : Array[String] )
    {
        require( args.length == 2 )
        val fin = new FileInputStream( args(0) )
        val in = new BufferedInputStream(fin)
        val decompressor = new BZip2CompressorInputStream(in)
        
        val conf = new Configuration()
        val fs = FileSystem.get(conf)        
        //val writer = new BlockCompressWriter( fs, conf, new Path(args(2)), Text.class, Text.class )
        val writer = createWriter( fs, conf, new Path(args(1)), new Text().getClass(), new Text().getClass(), BLOCK )
        
        var count = 0
        try
        {
            val source = Source.fromInputStream( decompressor )
            val parser = new XMLEventReader(source)

            while (parser.hasNext)
            {
                parser.next match
                {
                    case EvElemStart(_, "page", attrs, _) =>
                    {
                        val (title, id, revision, text) = parsePage( parser )
                        
                        writer.append( new Text(title), new Text(text) )
                        
                        count = count + 1
                        if ( count % 100 == 0 )
                        {
                            println( title + " " + count )
                        }
                    }
                    case _ =>
                }
                // Do something
            }
        }
        finally
        {
            decompressor.close()
            fin.close()
        }

        println( "Finished decompression.")
    }
}
