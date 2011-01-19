#include "sqldb.hpp"

#include <map>
#include <vector>
#include <algorithm>
#include <iostream>

#include <boost/scoped_ptr.hpp>
#include <boost/tuple/tuple_comparison.hpp>

struct Topic
{
    Topic( int count ) : m_count(count)
    {
    }
    
    int             m_count;
};

struct Word
{
    Word( int parentTopicCount ) : m_parentTopicCount(parentTopicCount)
    {
    }
    
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
    
    std::cout << "Loading topics" << std::endl;
    std::map<int, Topic> topics;
    {
        size_t count = 0;
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT id, wordCount FROM topics" ) );
        while(true)
        {
            boost::tuple<int, int> t;
            ise::sql::populateRowTuple( *rs, t );
            topics.insert( std::make_pair( t.get<0>(), Topic( t.get<1>() ) ) );

	        if ( ((++count) % 100000) == 0 ) std::cout << count << std::endl;
            if ( !rs->advance() ) break;
        }
    }
    std::cout << "  " << topics.size() << std::endl;
    
    std::cout << "Loading words" << std::endl;
    std::map<int, Word> words;
    {
        size_t count = 0;
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT id, parentTopicCount FROM words" ) );
        while(true)
        {
            boost::tuple<int, int> t;
            ise::sql::populateRowTuple( *rs, t );
            
            int wordId = t.get<0>();
            int wordCount = t.get<1>();
            
            if ( wordCount > 10 )
            {
                words.insert( std::make_pair( wordId, Word( wordCount ) ) );

	            if ( ((++count) % 100000) == 0 ) std::cout << count << " : " << t.get<0>() << ", " << t.get<1>() << std::endl;
            }
            if ( !rs->advance() ) break;
        }
    }
    std::cout << "  " << words.size() << std::endl;
    
    std::cout << "Loading associations" << std::endl;
    
    // Word id, topic id, word frequency in topic
    std::vector<boost::tuple<int, int, int> > wordAssocs;
    //std::map<int, std::vector<ParentTopic> > wordAssocs;
    {
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation" ) );
        size_t count = 0;
	    while (true)
	    {
		    boost::tuple<int, int, double, double> t;
		    ise::sql::populateRowTuple( *rs, t );
		
		    // Word id to (topic id, count)
		    //wordAssocs[t.get<1>()].push_back( ParentTopic(t.get<0>(), t.get<2>()) );
		    wordAssocs.push_back( boost::make_tuple( t.get<1>(), t.get<0>(), t.get<2>() ) );
		
		    if ( ((++count) % 1000000) == 0 ) std::cout << count << std::endl;
		
		    if ( !rs->advance() )
		    {
		        break;
            }
	    }
    }
    
    std::cout << "Sorting vector(!) " << std::endl;
    std::sort( wordAssocs.begin(), wordAssocs.end() );
    
    // Iterate over the word associations, calculating the weights for each term and only keeping the top N
}
