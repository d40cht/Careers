import org.scalatest.FunSuite

import java.io.{File, BufferedReader, FileReader}
import com.almworks.sqlite4java._

import org.apache.lucene.util.Version.LUCENE_30
import org.apache.lucene.analysis.Token
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.lucene.analysis.standard.StandardTokenizer

class DisambiguatorTest extends FunSuite
{
    test("Monbiot disambiguator test")
    {
        val testFileName = "./src/test/scala/data/monbiotTest.txt"
        
        val tokenizer = new StandardTokenizer( LUCENE_30, new BufferedReader( new FileReader( testFileName ) ) )
        
        var run = true
        var wordList : List[String] = Nil
        while ( run )
        {
            wordList = tokenizer.getAttribute(classOf[TermAttribute]).term() :: wordList
            run = tokenizer.incrementToken()
        }
        tokenizer.close()
        
        //println( wordList.reverse.toString )
        
        /*val dbFileName = 
        val db = new SQLiteConnection( new File(dbFileName) )
        
        val topicQuery = db.prepare( "SELECT t2.id, t2.name, t3.categoryId, t4.name FROM surfaceForms AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN categoryMembership AS t3 ON t1.topicId=t3.topicId INNER JOIN categories AS t4 ON t3.categoryId=t4.id WHERE t1.name=? ORDER BY t2.id, t3.categoryId" )*/
    }
}
