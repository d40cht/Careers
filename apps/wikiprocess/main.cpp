#include "sqldb.hpp"

#include <map>
#include <vector>
#include <algorithm>
#include <iostream>

#include <cmath>

#include <boost/scoped_ptr.hpp>
#include <boost/tuple/tuple_comparison.hpp>

#include "iseExceptions.hpp"

struct Topic
{
    Topic( int wordCount ) : m_wordCount(wordCount)
    {
    }
    
    int             m_wordCount;
};

struct Word
{
    Word( int topicCount, int parentTopicCount ) : m_inverseDocFrequency(std::log((float) topicCount / (float) parentTopicCount ) )
    {
    }
    
    float           m_inverseDocFrequency;
};

struct ParentTopic
{
    ParentTopic( int topicId, int termFrequency ) : m_topicId(topicId), m_termFrequency(termFrequency)
    {
    }
    
    int             m_topicId;
    int             m_termFrequency;
};

void run()
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alexw/AW/Careers/play.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbConnection> dbout( ise::sql::newSqliteConnection( "process.sqlite3" ) );
    dbout->execute( "CREATE TABLE topics( id INTEGER, title TEXT )" );
    dbout->execute( "CREATE TABLE words( id INTEGER, name TEXT )" );
    dbout->execute( "CREATE TABLE wordAssociation( topicId INTEGER, wordId INTEGER, termWeight REAL )" );



    dbout->execute( "BEGIN" );
    
    std::cout << "Loading topics" << std::endl;
    std::map<int, Topic> topics;
    {
        boost::scoped_ptr<ise::sql::PreparedStatement> p( dbout->preparedStatement( "INSERT INTO topics VALUES( ?, ? )" ) );
        size_t count = 0;
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT id, wordCount, title FROM topics" ) );
        while(true)
        {
            boost::tuple<int, int, std::string> t;
            ise::sql::populateRowTuple( *rs, t );
            
            int topicId = t.get<0>();
            int wordCount = t.get<1>();
            const std::string& text = t.get<2>();

            if ( wordCount > 30 )
            {
                p->execute( boost::make_tuple( topicId, text ) );
                topics.insert( std::make_pair( topicId, Topic(wordCount) ) );
    	        if ( ((++count) % 100000) == 0 ) std::cout << count << std::endl;
            }
            if ( !rs->advance() ) break;
        }
    }
    int numTopics = topics.size();
    std::cout << "  " << numTopics << std::endl;
    
    std::cout << "Committing changes..." << std::endl;
    dbout->execute( "COMMIT" );

    std::cout << "Loading words" << std::endl;
    dbout->execute( "BEGIN" );
    
    std::map<int, Word> words;
    {
        boost::scoped_ptr<ise::sql::PreparedStatement> p( dbout->preparedStatement( "INSERT INTO words VALUES( ?, ? )" ) );
        size_t count = 0;
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT id, parentTopicCount, name FROM words" ) );
        while(true)
        {
            boost::tuple<int, int, std::string> t;
            ise::sql::populateRowTuple( *rs, t );
            
            int wordId = t.get<0>();
            int numParentTopics = t.get<1>();
            const std::string& text = t.get<2>();
            
            if ( numParentTopics >= 2 )
            {
                if ( numParentTopics < 400000 )
                {
                    p->execute( boost::make_tuple( wordId, text ) );
                    words.insert( std::make_pair( wordId, Word( numTopics, numParentTopics ) ) );

	                if ( ((++count) % 100000) == 0 ) std::cout << count << " : " << t.get<0>() << ", " << t.get<1>() << std::endl;
                }
                else
                {
                    std::cout << numParentTopics << ": " << text << std::endl;
                }
            }
            if ( !rs->advance() ) break;
        }
    }
    std::cout << "  " << words.size() << std::endl;
    std::cout << "Comitting changes" << std::endl;
    dbout->execute( "COMMIT" );
    
    std::cout << "Loading associations" << std::endl;
    
    // Word id, topic id, word frequency in topic
    //std::vector<boost::tuple<int, int, int> > wordAssocs;
    //std::map<int, std::vector<ParentTopic> > wordAssocs;
    std::map<int, std::multimap<float, int> > wordAssocs;
    {
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation" ) );
        size_t count = 0;
	    while (true)
	    {
		    boost::tuple<int, int, double, double> t;
		    ise::sql::populateRowTuple( *rs, t );
		
		    // Word id to (topic id, count)
		    //wordAssocs[t.get<1>()].push_back( ParentTopic(t.get<0>(), t.get<2>()) );
		    int wordId = t.get<1>();
		    int topicId = t.get<0>();
		    int wordCountInTopic = t.get<2>();
		    //float wordImportanceInTopic = t.get<2>();

            if ( words.find(wordId) != words.end() && topics.find(topicId) != topics.end() )
            {		    
		        float wordInverseDocFrequency = words.find(wordId)->second.m_inverseDocFrequency;
		        int wordsInTopic = topics.find(topicId)->second.m_wordCount;
		        
		        float wordImportanceInTopic = ((float) wordCountInTopic) / ((float) wordsInTopic);
		        
		        float tfIdf = wordImportanceInTopic * wordInverseDocFrequency;
		        
		        //std::cout << "** " << wordInverseDocFrequency << " " << tfIdf << std::endl;
		        
		        //wordAssocs.push_back( boost::make_tuple( t.get<1>(), t.get<0>(), tfIdf ) );
		        std::multimap<float, int>& wordMap = wordAssocs[wordId];
		        
		        wordMap.insert( std::make_pair( tfIdf, topicId ) );
		        
		        // Keep only the highest importance topics per word
		        if ( wordMap.size() > 200 )
		        {
		            wordMap.erase( wordMap.begin() );
		        }
            }
		
		    if ( ((++count) % 1000000) == 0 ) std::cout << count << std::endl;
		
		    if ( !rs->advance() )
		    {
		        break;
            }
	    }
    }
    
    std::cout << "Dumping cut down topic map" << std::endl;
    
    dbout->execute( "BEGIN" );
    int limitedCount = 0;
    {
        boost::scoped_ptr<ise::sql::PreparedStatement> p( dbout->preparedStatement( "INSERT INTO wordAssociation VALUES( ?, ?, ? )" ) );
        for ( std::map<int, std::multimap<float, int> >::iterator it = wordAssocs.begin(); it != wordAssocs.end(); ++it )
        {
            int wordId = it->first;
            for ( std::multimap<float, int>::iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2 )
            {
                double distance = it2->first;
                int topicId = it2->second;
                p->execute( boost::make_tuple( topicId, wordId, distance ) );
            }
            limitedCount += it->second.size();
        }
    }
    
    std::cout << "  " << limitedCount << " topics associations" << std::endl;
    std::cout << "Committing word associations" << std::endl;
    dbout->execute( "COMMIT" );
    std::cout << "  complete..." << std::endl;
    
    std::cout << "Building indices" << std::endl;

    dbout->execute("CREATE INDEX i1 ON topics(id)");
    dbout->execute("CREATE INDEX i2 ON words(id)");
    dbout->execute("CREATE INDEX i3 ON words(name)");
    dbout->execute("CREATE INDEX i4 ON wordAssociation(wordId)");
}

void run2()
{
    //CREATE TABLE wordAssociation( topicId INTEGER, wordId INTEGER, wordInTopicCount INTEGER, termWeight REAL );
    
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "/home/alexw/AW/Careers/new.sqlite3" ) );
    
    //boost::scoped_ptr<ise::sql::DbConnection> dbout( ise::sql::newSqliteConnection( "process.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT * FROM wordAssociation2" ) );
    
    std::vector<boost::tuple<int, int, int> > data;
    {
        std::cout << "Loading data" << std::endl;
        size_t count = 0;
        
        
	    while (true)
	    {
		    boost::tuple<int, int, double, double> t;
		    ise::sql::populateRowTuple( *rs, t );
		    
		    data.push_back( boost::make_tuple( t.get<0>(), t.get<1>(), t.get<2>() ) );
    
            if ( ((++count) % 1000000) == 0 ) std::cout << count << std::endl;
		
		    if ( !rs->advance() )
		    {
		        break;
            }
	    }
    }
	    
    std::cout << "Sorting data" << std::endl;
    std::sort( data.begin(), data.end() );
	 
    {
        std::cout << "Reloading data back into wordAssociation table" << std::endl;
	    db->execute( "BEGIN" );
	    
        boost::scoped_ptr<ise::sql::PreparedStatement> p( db->preparedStatement( "INSERT INTO wordAssociation VALUES( ?, ?, ?, ? )" ) );   
        

        int c = 0;
	    boost::tuple<int, int, int> topicWordAssoc( 0, 0, 0 );
	    for ( std::vector<boost::tuple<int, int, int> >::iterator it = data.begin(); it != data.end(); ++it )
	    {
	        if ( topicWordAssoc.get<0>() != it->get<0>() || topicWordAssoc.get<1>() != it->get<1>() )
	        {
	            // Push out here
	            p->execute( boost::make_tuple( topicWordAssoc.get<0>(), topicWordAssoc.get<1>(), topicWordAssoc.get<2>(), 0.0 ) );
	            if ( (++c % 1000000) == 0 ) std::cout << "  count: " << c << std::endl;  
	            topicWordAssoc = *it;
	        }
	        else
	        {
	            topicWordAssoc.get<2>() += it->get<2>();
	        }
	    }
	    db->execute( "COMMIT" );
    }
    
}

int main( int argc, char** argv )
{
    try
    {
        run2();
    }
    catch ( ise::exceptions::Generic& e )
    {   
        std::cout << e.str() << std::endl;
    }
    
    return 0;
}
