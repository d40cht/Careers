
import java.io.File
import com.almworks.sqlite4java._

object SqliteWrapper
{
	trait TypedCol[T]
	{
		var v : Option[T] = None
		def assign( res : SQLiteStatement, index : Int )
	}

	sealed trait HList
	{
		def assign( res : SQLiteStatement, index : Int )
	}

	final case class HCons[H <: TypedCol[_], T <: HList]( var head : H, tail : T ) extends HList
	{
		def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
		def assign( res : SQLiteStatement, index : Int )
		{
		    head.assign( res, index )
		    tail.assign( res, index+1 )
		}
	}

	final class HNil extends HList
	{
		def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
		def assign( res : SQLiteStatement, index : Int )
		{
		}
	}

	type ::[H <: TypedCol[_], T <: HList] = HCons[H, T]

	val HNil = new HNil()



	final class IntCol extends TypedCol[Int]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnInt(index) ) }
	}

	final class DoubleCol extends TypedCol[Double]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnDouble(index) ) }
	}

	final class StringCol extends TypedCol[String]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnString(index) ) }
	}

	trait TypedColMaker[T]
	{
		def build() : TypedCol[T]
	}

	object TypedColMaker
	{
		implicit object IntColMaker extends TypedColMaker[Int]
		{
		    def build() : TypedCol[Int] = new IntCol()
		}
		implicit object DoubleColMaker extends TypedColMaker[Double]
		{
		    def build() : TypedCol[Double] = new DoubleCol()
		}
		implicit object StringColMaker extends TypedColMaker[String]
		{
		    def build() : TypedCol[String] = new StringCol()
		}
	}

	def Col[T : TypedColMaker]() = implicitly[TypedColMaker[T]].build()

	// Hideousness. Improve as Scala metaprogramming ability improves
	def _1[H <: TypedCol[_], T <: HList]( t : HCons[H, T] ) = t.head.v
	def _2[H1 <: TypedCol[_], H2 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, T]] ) = t.tail.head.v
	def _3[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, T]]] ) = t.tail.tail.head.v
	def _4[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], H4 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, HCons[H4, T]]]] ) = t.tail.tail.tail.head.v
	def _5[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], H4 <: TypedCol[_], H5 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, HCons[H4, HCons[H5, T]]]]] ) = t.tail.tail.tail.tail.head.v
	def _6[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], H4 <: TypedCol[_], H5 <: TypedCol[_], H6 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, HCons[H4, HCons[H5, HCons[H6, T]]]]]] ) = t.tail.tail.tail.tail.tail.head.v
	def _7[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], H4 <: TypedCol[_], H5 <: TypedCol[_], H6 <: TypedCol[_], H7 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, HCons[H4, HCons[H5, HCons[H6, HCons[H7, T]]]]]]] ) = t.tail.tail.tail.tail.tail.tail.head.v
	def _8[H1 <: TypedCol[_], H2 <: TypedCol[_], H3 <: TypedCol[_], H4 <: TypedCol[_], H5 <: TypedCol[_], H6 <: TypedCol[_], H7 <: TypedCol[_], H8 <: TypedCol[_], T <: HList]( t : HCons[H1, HCons[H2, HCons[H3, HCons[H4, HCons[H5, HCons[H6, HCons[H7, HCons[H8, T]]]]]]]] ) = t.tail.tail.tail.tail.tail.tail.tail.head.v

	final class DataWrapper[T <: HList]( var row : T )
	{
		def assign( res : SQLiteStatement ) { row.assign( res, 0 ) }
	}
	
	final class SQLiteWrapper( dbFile : File )
    {
        val conn = new SQLiteConnection( dbFile )
        conn.open()
        
        def exec( statement : String )
        {
            conn.exec( statement )
        }
        
        def prepare[T <: HList]( query : String, row : T ) =
        {
            new PreparedStatement(query, row)
        }
        
        // TODO: Parameterise with tuple type
        // make applicable to for comprehensions (implement filter, map, flatMap)
        final class PreparedStatement[T <: HList]( query : String, var row : T )
        {
            val statement = conn.prepare( query )
            
            private def bindRec( index : Int, params : List[Any] )
            {
                println( "Value " + params.head )
                // TODO: Does this need a pattern match?
                params.head match
                {
                    case v : Int => statement.bind( index, v )
                    case v : String => statement.bind( index, v )
                    case v : Double => statement.bind( index, v )
                    case _ => throw new ClassCastException( "Unsupported type in bind." )
                }
                
                if ( params.tail != Nil )
                {
                    bindRec( index+1, params.tail )
                }
            }
            
            def bind( args : Any* )
            {
                bindRec( 1, args.toList )
            }
            
            def exec( args : Any* )
            {
                bindRec( 1, args.toList )
                step()
                reset()
            }
            
            def reset()
            {
                statement.reset()
            }
            
            def step() : Boolean =
            {
            	val success = statement.step()
            	row.assign( statement, 0 )
                return success
            }
        }
    }
}

