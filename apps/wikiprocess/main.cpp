#include "sqldb.hpp"

#include <iostream>
#include <boost/scoped_ptr.hpp>

int main( int argc, char** argv )
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alexw/AW/Careers/play.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation" ) );
    
    size_t count = 0;
    std::map<size_t, std::pair<size_t, size_t> > wordAssocs;
	while (true)
	{
		boost::tuple<int, int, double, double> t;
		ise::sql::populateRowTuple( *rs, t );
		
		// Word id to (topic id, count)
		wordAssocs[t.get<1>()].push_back( std::make_pair(t.get<0>(), t.get<2>()) );
		
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
