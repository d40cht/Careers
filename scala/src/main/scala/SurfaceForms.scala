import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._

class SurfaceFormsMapper extends Mapper[Text, Text, Text, Text]
{
    override def map(key : Text, value : Text, context : Mapper[Text, Text, Text, Text]#Context) =
    {
        //context.write( surfaceForm, topic )
    }
}

class SurfaceFormsReducer extends Reducer[Text, Text, Text, Text]
{
    override def reduce(key : Text, values : java.lang.Iterable[Text], context : Reducer[Text, Text, Text, Text]#Context) = 
    {
        // context.write( key, value )
    }
}


object SurfaceForms
{
    def main(args:Array[String]) : Unit =
    {
        val conf = new Configuration()
        val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
        
        if ( otherArgs.length != 2 )
        {
            println( "Usage: surfaceforms <in> <out>")
            return 2
        }
        
        val job = new Job(conf, "Surface forms")
        
        job.setJarByClass(classOf[SurfaceFormsMapper])
        job.setMapperClass(classOf[SurfaceFormsMapper])
        job.setCombinerClass(classOf[SurfaceFormsReducer])
        job.setReducerClass(classOf[SurfaceFormsReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        
        if ( job.waitForCompletion(true) ) 0 else 1
    }
}
