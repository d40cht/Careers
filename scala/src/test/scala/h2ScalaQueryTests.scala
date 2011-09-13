package org.seacourt.tests

import org.scalatest.FunSuite

import java.io.{File}
import java.sql.{Timestamp, Blob}
import javax.sql.rowset.serial.SerialBlob

import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession

import org.scalaquery.ql.extended.ExtendedColumnOption.AutoInc
import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.basic.BasicDriver.Implicit._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._

import org.seacourt.utility._

import sbinary._
import sbinary.Operations._

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
                
                val range = 0 until 15
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
                
                val allRows = for ( row <- CVs if row.sqr > 10.0 ) yield row
                val maxValue = allRows.map( row => row.sqr.max ).first
                val minValue = allRows.map( row => row.sqr.min ).first
                val count = allRows.map( row => ColumnOps.CountAll(row) ).first
            }
        } )
    }
}


class Point( val x : Double, val y : Double )
{
    def dist( other : Point ) =
    {
        val xd = (x-other.x)
        val yd = (y-other.y)
        
        xd*xd + yd*yd
    }
}

object SerializationProtocol extends sbinary.DefaultProtocol
{
    implicit object PointFormat extends Format[Point]
    {
        def reads(in : Input) = new Point( read[Double](in), read[Double](in) )
        def writes(out : Output, p : Point )
        {
            write(out, p.x)
            write(out, p.y)
        }
    }
}

object Points extends Table[(Int, Blob)]("Points")
{
    def id      = column[Int]("id", O PrimaryKey)
    def point   = column[Blob]("point")
    
    def * = id ~ point
}

object Matches extends Table[(Int, Int, Double)]("Matches")
{
    def fromId      = column[Int]("fromId")
    def toId        = column[Int]("toId")
    def similarity  = column[Double]("distance")
    
    def * = fromId ~ toId ~ similarity
    def pk = primaryKey("pk_Matches", fromId ~ toId )
}


class DistanceManager()
{
    val maxRows = 5
    
    var nextId = 0
    import SerializationProtocol._
    
    private def update( fromId : Int, toId : Int, sim : Double )
    {
        val relevantRows = for ( row <- Matches if row.fromId === fromId ) yield row
        val rowCount = relevantRows.map( row => row.toId.count ).first
        val (minSim, minId) = ( for ( row <- relevantRows; _ <- Query orderBy( row.similarity asc ) ) yield row.similarity ~ row.toId ).firstOption.getOrElse( (0.0, 0) )
        
        assert( rowCount <= maxRows )
        
        if ( rowCount < maxRows || sim > minSim )
        {
            if ( rowCount == maxRows )
            {
                Matches.filter( row => row.fromId === fromId && row.toId === minId ).mutate( m => m.delete )
            }
            
            val cols = Matches.fromId ~ Matches.toId ~ Matches.similarity
            assert( fromId != toId )
            cols.insert( fromId, toId, sim )
        }
    }
    

    def addElement( p : Point )
    {
        //println( nextId )
        val serialized = toByteArray(p)
        val fromId = nextId
        nextId += 1
        Points.insert( fromId, new SerialBlob(serialized) )
        
        val allPoints = for ( row <- Points ) yield row
        
        allPoints.foreach( row =>
        {
            val toId = row._1
            if ( fromId != toId )
            {
                val blob = row._2
                val rowPoint = fromByteArray[Point]( blob.getBytes(1, blob.length().toInt) )
                val dist = rowPoint.dist( p )
                
                update( fromId, toId, dist )
                update( toId, fromId, dist )
            }
        } )
    }
}


class H2DistanceTest extends FunSuite
{
    test( "H2 and Scalaquery distance test", TestTags.unitTests )
    {
        Utils.withTemporaryDirectory( dirName =>
        {
            val dbFileName = new File( dirName, "testdb" )
            val db = Database.forURL("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1".format( dbFileName.toString ), driver = "org.h2.Driver")
            
            db withSession 
            {
                val d = new DistanceManager()
                
                Points.ddl.create
                Matches.ddl.create
                
                val rng = new scala.util.Random()
                for ( iterations <- 0 until 100 )
                {
                    // [0.0 - 1.0]
                    val x = rng.nextDouble()
                    val y = rng.nextDouble()
                    
                    val np = new Point(x, y)
                    d.addElement( np )
                }
            }
        } )
    }
}


