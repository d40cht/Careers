
/*
object Disambiguator
{
    def main( args : Array[String] )
    {
        require( args.length === 2 )
        val dbFileName = args(0)
        val testFileName = args(1)
        val db = new SQLiteConnection( new File(dbFileName) )
        
        val topicQuery = db.prepare( "SELECT t2.id, t2.name, t3.categoryId, t4.name FROM surfaceForms AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id INNER JOIN categoryMembership AS t3 ON t1.topicId=t3.topicId INNER JOIN categories AS t4 ON t3.categoryId=t4.id WHERE t1.name=? ORDER BY t2.id, t3.categoryId" )
        
    }
}
*/

