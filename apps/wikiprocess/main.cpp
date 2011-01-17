#include "sqldb.hpp"

#include <map>
#include <vector>
#include <iostream>
#include <boost/scoped_ptr.hpp>

struct Topic
{
    Topic( const std::string text, int count ) : m_text(text), m_count(count)
    {
    }
    
    std::string     m_text;
    int             m_count;
};

struct Word
{
    Word( const std::string& name, int parentTopicCount ) : m_name(name), m_parentTopicCount(parentTopicCount)
    {
    }
    
    std::string     m_name;
    int             m_parentTopicCount;
};

struct ParentTopic
{
    ParentTopic( int topicId, int termFrequency ) : m_topicId(topicId), m_termFrequency(termFrequency)
    {
    }
    
    int             m_topicId;
    int             m_termFrequency;
};

int main( int argc, char** argv )
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alexw/AW/Careers/play.sqlite3" ) );
    
    std::map<int, Topic> topics;
    {
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM topics" ) );
        while(true)
        {
            boost::tuple<int, std::string, int> t;
            ise::sql::populateRowTuple( *rs, t );
            topics.insert( std::make_pair( t.get<0>(), Topic( t.get<1>(), t.get<2>() ) ) );
            if ( !rs->advance() ) break;
        }
    }
    
    std::map<int, Word> words;
    {
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM words" ) );
        while(true)
        {
            boost::tuple<int, std::string, int, double> t;
            ise::sql::populateRowTuple( *rs, t );
            words.insert( std::make_pair( t.get<0>(), Word( t.get<1>(), t.get<2>() ) ) );
            if ( !rs->advance() ) break;
        }
    }
    
    std::map<int, std::vector<ParentTopic> > wordAssocs;
    {
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation" ) );
        size_t count = 0;
	    while (true)
	    {
		    boost::tuple<int, int, double, double> t;
		    ise::sql::populateRowTuple( *rs, t );
		
		    // Word id to (topic id, count)
		    wordAssocs[t.get<1>()].push_back( ParentTopic(t.get<0>(), t.get<2>()) );
		
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
}
