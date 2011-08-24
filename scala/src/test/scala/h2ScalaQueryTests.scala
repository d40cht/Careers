package org.seacourt.tests

import org.scalatest.FunSuite

import java.io.{File}

import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession

import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.basic.BasicDriver.Implicit._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._

import org.seacourt.utility._

object CVs extends Table[(Int, String, Double)]("CVs")
{
    def id      =   column[Int]("id", O NotNull)
    def str     =   column[String]("str")
    def sqr     =   column[Double]("sqr")
    
    def * = id ~ str ~ sqr
}

class H2DbDebugTest extends FunSuite
{
    test( "H2 and Scalaquery test1", TestTags.unitTests )
    {
        Utils.withTemporaryDirectory( dirName =>
        {
            val dbFileName = new File( dirName, "testdb" )
            val db = Database.forURL("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1".format( dbFileName.toString ), driver = "org.h2.Driver")
            
            db withSession
            {
                CVs.ddl.create
                
                val range = 0 until 5
                for ( id <- range )
                {
                    val str = id.toString
                    val sqr = id.toDouble * id.toDouble
                    
                    CVs insert (id, str, sqr)
                }
                
                val rows = for
                {
                    row <- CVs
                    __ <- Query.orderBy( row.id desc )
                } yield row.id ~ row.str ~ row.sqr
                
                val expected = range.reverse.map( x => (x, x.toString, x.toDouble * x.toDouble) )
                assert( expected === rows.list.map( x => (x._1, x._2, x._3) ) )
            }
        } )
    }
}

