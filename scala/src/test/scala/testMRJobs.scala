import org.scalatest.FunSuite
import org.scalatest.Tag

import scala.collection.immutable.TreeSet
import scala.io.Source._

import org.seacourt.utility._
import org.seacourt.mapreducejobs._

import org.apache.hadoop.io.SequenceFile.{createWriter, Reader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

class CategoryMembershipTest extends FunSuite
{
    val parseTestFile = "./src/test/scala/data/parsetest.txt"
    
    /*test("Temp test")
    {
        val runtime = Runtime.getRuntime
        
        val conf = new Configuration()
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"))
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"))
        val fs = FileSystem.get(conf)
        
        val reader = new Reader(fs, new Path("hdfs://shinigami.lan.ise-oxford.com:54310/user/alexw/enwikifull.seq"), conf)
        //val reader = new Reader(fs, new Path("./enwikifull.seq"), conf)
        
        val title = new Text()
        val topic = new Text()
        
        var done = false
        var count = 0
        while ( !done && reader.next( title, topic ) )
        {
            if ( title.toString.startsWith("28") ) println( title.toString )
            if ( title.toString == "28 Days Later: The Soundtrack Album" ) 
            {
                println( count )
                done = true
                
                val expectedResultsL =
                    "Category:2003 soundtracks" ::
                    "Category:Albums by British artists" ::
                    "Category:Film soundtracks" ::
                    "Main:Godspeed You! Black Emperor" ::
                    "Main:Post-rock" ::
                    "Main:East Hastings" ::
                    "Main:Blue States (band)" ::
                    "Main:Grandaddy" ::
                    "Main:Brian Eno" ::
                    "Main:Film score" ::
                    "Main:28 Days Later" ::
                    "Main:2002 in film" ::
                    "Main:Soundtrack album" ::
                    "Main:Millions" ::
                    "Main:The Beach (film)" ::
                    "Main:Danny Boyle" ::
                    "Main:Sputnikmusic" ::
                    "Main:Allmusic" ::
                    "Main:Classical music" ::
                    "Main:Electronica" ::
                    "Main:Ambient Music" ::
                    "Main:Post-Rock" ::
                    "Main:Rock music" ::
                    "Main:John Murphy (composer)" :: Nil
                    
                var expectedResults = new TreeSet[String]
                for ( e <- expectedResultsL ) expectedResults = expectedResults + e
                    
                val topicTitle = "Test title"
                val topicText = fromFile(parseTestFile).getLines.mkString
                
                val v = new CategoriesAndContexts.JobMapper()
                
                var results : List[(String, String)] = Nil
                v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
                
                for ( (exp, res) <- expectedResults.zip( results.reverse ) )
                {
                    assert( "Main:Test title" == res._1 )
                    assert( exp === res._2 )
                }
            }
            count = count +1
            if ( (count % 10000) == 0 ) println( count )
        }
    }*/
    
    test("Test category and context parsing")
    {
        // Remove duplicates!
        val expectedResultsL =
            "Category:2003 soundtracks" ::
            "Category:Albums by British artists" ::
            "Category:Film soundtracks" ::
            "Main:Godspeed You! Black Emperor" ::
            "Main:Post-rock" ::
            "Main:East Hastings" ::
            "Main:Blue States (band)" ::
            "Main:Grandaddy" ::
            "Main:Brian Eno" ::
            "Main:Film score" ::
            "Main:28 Days Later" ::
            "Main:2002 in film" ::
            "Main:Soundtrack album" ::
            "Main:Millions" ::
            "Main:The Beach (film)" ::
            "Main:Danny Boyle" ::
            "Main:Sputnikmusic" ::
            "Main:Allmusic" ::
            "Main:Classical music" ::
            "Main:Electronica" ::
            "Main:Ambient Music" ::
            "Main:Post-Rock" ::
            "Main:Rock music" ::
            "Main:John Murphy (composer)" :: Nil
            
        var expectedResults = new TreeSet[String]
        for ( e <- expectedResultsL ) expectedResults = expectedResults + e
            
        val topicTitle = "Test title"
        val topicText = fromFile(parseTestFile).getLines.mkString
        
        val v = new CategoriesAndContexts.JobMapper()
        
        var results : List[(String, String)] = Nil
        v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
        
        assert( results.length === expectedResults.size )
        for ( (exp, res) <- expectedResults.zip( results.reverse ) )
        {
            assert( "Main:Test title" === res._1 )
            assert( exp === res._2 )
        }
    }
    
    test("Test redirect parsing")
    {
        // Not a redirect page. Should not add to the list
        {
            val v = new RedirectParser.JobMapper()
            
            val topicTitle = "Test title"
            val topicText = fromFile(parseTestFile).getLines.mkString
            
            var results : List[(String, String)] = Nil
            v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
            
            assert( results === Nil )
        }
        
        // A redirect page. Expect it to show up
        {
            val v = new RedirectParser.JobMapper()
            
            val topicTitle = "Test title"
            val topicText = "#REDIRECT [[Academic acceleration]] {{R from other capitalisation}}"
            
            var results : List[(String, String)] = Nil
            v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
            
            println( results(0) )
            
            assert( results(0)._1 == ("Main:Test title") )
            assert( results(0)._2 == ("Main:Academic acceleration") )
        }
    }
    
    test("Test surface form parsing")
    {
        val expectedResults =
            ("the rotted", "Main:The Rotted") ::
            ("the beach", "Main:The Beach (film)") ::
            ("sputnikmusic", "Main:Sputnikmusic") ::
            ("soundtrack", "Main:Soundtrack album") ::
            ("rock", "Main:Rock music") ::
            ("richard marlow", "Main:Richard Marlow") ::
            ("post rock", "Main:Post-rock") ::
            ("post rock", "Main:Post-Rock") ::
            ("perri alleyne", "Main:Perri Alleyne") ::
            ("original score", "Main:Film score") ::
            ("millions", "Main:Millions") ::
            ("metro 2033", "Main:Metro 2033") ::
            ("kick ass", "Main:Kick-Ass (film)") ::
            ("john murphy", "Main:John Murphy (composer)") ::
            ("jacknife lee", "Main:Jacknife Lee") ::
            ("instrumental", "Main:Instrumental") ::
            ("grandaddy", "Main:Grandaddy") ::
            ("godspeed you black emperor", "Main:Godspeed You! Black Emperor") ::
            ("faures requiem in d minor", "Main:Requiem (Faure)") ::
            ("electronica", "Main:Electronica") ::
            ("east hastings", "Main:East Hastings") ::
            ("death metal", "Main:Death Metal") ::
            ("danny boyle", "Main:Danny Boyle") ::
            ("classical", "Main:Classical music") ::
            ("brian eno", "Main:Brian Eno") ::
            ("blue states", "Main:Blue States (band)") ::
            ("ave maria", "Main:Ave Maria (Gounod)") ::
            ("ambient music", "Main:Ambient Music") ::
            ("allmusic", "Main:Allmusic") ::
            ("abide with me", "Main:Abide With Me") ::
            ("a.m. 180", "Main:A.M. 180") ::
            ("28 weeks later", "Main:28 Weeks Later") ::
            ("28 days later", "Main:28 Days Later") ::
            ("2002 film", "Main:2002 in film") :: Nil


        // TODO: Remove duplicates in mapper
        val v = new SurfaceFormsGleaner.JobMapper()
        
        val topicTitle = "Test title"
        val topicText = fromFile(parseTestFile).getLines.mkString
        
        var results : List[(String, String)] = Nil
        v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
        
        //println( results )
        for ( (result, expected) <- results.zip(expectedResults))
        {
            assert( result._1 === expected._1 )
            assert( result._2 === expected._2 )
        }
    }
    
    test("Test counting words in topics", Tag("UnitTests"))
    {
        val v = new WordInTopicCounter.JobMapper()
        
        val topicTitle = "Test title"
        val topicText = fromFile(parseTestFile).getLines.mkString
        
        //var results : List[(String, Int)] = Nil
        var words = new TreeSet[String]
        v.mapWork( topicTitle, topicText, (word, count) =>
        {
            assert( count === 1 )
            assert( !words.contains(word) )
            words = words + word
        } )
        
        assert( words.contains( "parents" ) )
        assert( words.contains( "there" ) )
        assert( words.contains( "alleyne" ) )
    }

}
