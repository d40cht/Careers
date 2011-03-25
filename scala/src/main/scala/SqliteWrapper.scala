
import java.io.File
import com.almworks.sqlite4java._

object SqliteWrapper
{

	def deepCopy[A](a: A)(implicit m: reflect.Manifest[A]): A = util.Marshal.load[A](util.Marshal.dump(a))
	
	trait TypedCol[T]
	{
		var v : Option[T]
		def assign( res : SQLiteStatement, index : Int )
		def copy : TypedCol[T]
	}

	sealed trait HList
	{
		type SelfType <: TypedCol[_]
		def assign( res : SQLiteStatement, index : Int )
		def copy : HList
	}

	final case class HCons[H <: TypedCol[_], T <: HList]( var head : H, tail : T ) extends HList
	{
		def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
		def assign( res : SQLiteStatement, index : Int )
		{
		    head.assign( res, index )
		    tail.assign( res, index+1 )
		}
		override def copy = HCons(head.copy, tail.copy)
	}

	final class HNil extends HList
	{
		def ::[T <: TypedCol[_]](v : T) = HCons(v, this)
		def assign( res : SQLiteStatement, index : Int ) {}
		override def copy = HNil
	}

	type ::[H <: TypedCol[_], T <: HList] = HCons[H, T]

	val HNil = new HNil()



	final class IntCol( var v : Option[Int] = None ) extends TypedCol[Int]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnInt(index) ) }
		def copy = new IntCol( v match { case Some(x) => Some(x); case None => None } )
	}

	final class DoubleCol( var v : Option[Double] = None ) extends TypedCol[Double]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnDouble(index) ) }
		def copy = new DoubleCol( v match { case Some(x) => Some(x); case None => None } )
	}

	final class StringCol( var v : Option[String] = None ) extends TypedCol[String]
	{
		def assign( res : SQLiteStatement, index : Int ) { v = Some( res.columnString(index) ) }
		def copy = new StringCol( v match { case Some(x) => Some(x); case None => None } )
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
	def _1[H, T <: HList]( t : HCons[TypedCol[H], T] ) = t.head.v
	def _2[H1, H2, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], T]] ) = t.tail.head.v
	def _3[H1, H2, H3, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], T]]] ) = t.tail.tail.head.v
	def _4[H1, H2, H3, H4, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], HCons[TypedCol[H4], T]]]] ) = t.tail.tail.tail.head.v
	def _5[H1, H2, H3, H4, H5, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], HCons[TypedCol[H4], HCons[TypedCol[H5], T]]]]] ) = t.tail.tail.tail.tail.head.v
	def _6[H1, H2, H3, H4, H5, H6, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], HCons[TypedCol[H4], HCons[TypedCol[H5], HCons[TypedCol[H6], T]]]]]] ) = t.tail.tail.tail.tail.tail.head.v
	def _7[H1, H2, H3, H4, H5, H6, H7, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], HCons[TypedCol[H4], HCons[TypedCol[H5], HCons[TypedCol[H6], HCons[TypedCol[H7], T]]]]]]] ) = t.tail.tail.tail.tail.tail.tail.head.v
	def _8[H1, H2, H3, H4, H5, H6, H7, H8, T <: HList]( t : HCons[TypedCol[H1], HCons[TypedCol[H2], HCons[TypedCol[H3], HCons[TypedCol[H4], HCons[TypedCol[H5], HCons[TypedCol[H6], HCons[TypedCol[H7], HCons[TypedCol[H8], T]]]]]]]] ) = t.tail.tail.tail.tail.tail.tail.tail.head.v

	final class DataWrapper[T <: HList]( var row : T )
	{
		def assign( res : SQLiteStatement ) { row.assign( res, 0 ) }
	}
	
	final class SQLiteWrapper( dbFile : File )
    {
        val conn = new SQLiteConnection( dbFile )
        conn.open()
        
        def dispose()
        {
        	conn.dispose()
        }
        
        def exec( statement : String )
        {
            conn.exec( statement )
        }
        
        def getChanges() = conn.getChanges()
        
        def prepare[T <: HList]( query : String, row : T ) =
        {
            new PreparedStatement(query, row)
        }
        
        // TODO: Parameterise with tuple type
        // make applicable to for comprehensions (implement filter, map, flatMap)
        sealed class PreparedStatementBase[T <: HList]( query : String, var row : T )
        {
            protected val statement = conn.prepare( query )
            
            private def bindRec( index : Int, params : List[Any] )
            {
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
            
            protected def step() : Boolean =
            {
            	val success = statement.step()
            	if ( success )
            	{
            		row.assign( statement, 0 )
				}
                return success
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
        }
        
        final class PreparedStatement[T <: HList]( query : String, row : T ) extends PreparedStatementBase( query, row ) with Iterator[T]
        {
        	var nextRow : Option[T] = None
        	var currRow : Option[T] = None
        	var init = true
        	
        	private def advance()
        	{
        		if ( step() )
        		{
        			nextRow = Some(row)
        		}
        		else
        		{
        			nextRow = None
        			reset()
        		}
        	}

        	def hasNext = 
        	{
        		if ( init )
        		{
        			advance()
        			init = false
        		}
        		nextRow match { case Some(x) => true; case None => false }
			}
        	def next =
        	{
        		nextRow match
        		{
        			case Some(x) => currRow = Some(x.copy.asInstanceOf[T])
        			case None => currRow = None
        		}
        		advance(); currRow.get
			}
		}
    }
}

