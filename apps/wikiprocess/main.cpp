#include "sqldb.hpp"

#include <iostream>
#include <boost/scoped_ptr.hpp>

int main( int argc, char** argv )
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alexw/AW/Careers/play.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation" ) );
    
    size_t count = 0;
	while (true)
	{
		boost::tuple<int, int, double, double> t;
		ise::sql::populateRowTuple( *rs, t );
		
		if ( ((++count) % 1000000) == 0 )
		{
		    std::cout << count << std::endl;
		}
		
		if ( !rs->advance() )
		{
		    break;
        }
	}
}
