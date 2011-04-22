import org.scalatest.FunSuite
import scala.io.Source._

import org.seacourt.mapreducejobs._

class CategoryMembershipTest extends FunSuite
{
    val parseTestFile = "./src/test/scala/data/parsetest.txt"
    
    test("test1")
    {
        val expectedResults =
            "Category:2003 soundtracks" ::
            "Category:Albums by British artists" ::
            "Category:Film soundtracks" ::
            "Main:Godspeed You! Black Emperor" ::
            "Main:Post-rock" ::
            "Main:East Hastings" ::
            "Main:Blue States (band)" ::
            "Main:Grandaddy" ::
            "Main:Brian Eno" ::
            "Main:John Murphy (composer)" ::
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
            
        val topicTitle = "Test title"
        val topicText = fromFile(parseTestFile).getLines.mkString
        
        val v = new CategoriesAndContexts.JobMapper()
        
        var results : List[(String, String)] = Nil
        v.mapWork( topicTitle, topicText, (x, y) => results = (x,y) :: results )
        
        for ( (exp, res) <- expectedResults.zip( results) )
        {
            assert( "Main:Test title" == res._1 )
            assert( exp === res._2 )
        }
    }
}
