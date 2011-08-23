import org.scalatest.FunSuite
import org.scalatest.Tag

import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession

import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.basic.BasicDriver.Implicit._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._

object CVs extends Table[(Int, String, Double)]("CVs")
{
    def id      =   column[Int]("id", O NotNull)
    def str     =   column[String]("str")
    def sqr     =   column[Double]("sqr")
    
    def * = id ~ str ~ sqr
}

class H2DbDebugTest extends FunSuite
{
    test( "Simple H2 and Scalaquery test", Tag("UnitTests") )
    {
        val db = Database.forURL("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
        
        db withSession
        {
            CVs.ddl.create
            
            for ( id <- 0 until 12 )
            {
                val str = id.toString
                val sqr = id.toDouble * id.toDouble
                
                CVs insert (id, str, sqr)
            }
        }
    }
}

