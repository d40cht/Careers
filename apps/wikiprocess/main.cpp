#include "sqldb.hpp"

#include <iostream>
#include <boost/scoped_ptr.hpp>

int main( int argc, char** argv )
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alex/Devel/AW/career/cvscrape.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM cvText LIMIT 10" ) );
    
    size_t count = 0;
	while (true)
	{
		boost::tuple<int, int, std::string> t;
		ise::sql::populateRowTuple( *rs, t );
		
		if ( ((++count) % 1000) == 0 )
		{
		    std::cout << count << std::endl;
		}
		
		if ( !rs->advance() )
		{
		    break;
        }
	}
}
