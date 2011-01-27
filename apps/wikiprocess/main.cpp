#include "sqldb.hpp"

#include <map>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sstream>

#include <cmath>

#include <boost/scoped_ptr.hpp>
#include <boost/tuple/tuple_comparison.hpp>

#include "iseExceptions.hpp"


#if 0

CREATE TABLE words2( id INTEGER, name TEXT, parentTopicCount INTEGER, inverseDocumentFrequency REAL );
INSERT INTO words2 SELECT MIN(id), name, SUM(parentTopicCount), 0 from words WHERE parentTopicCount>10 GROUP BY name;


CREATE TABLE idtrans( idFrom INTEGER, idTo INTEGER );
INSERT INTO idtrans SELECT t1.id, t2.id FROM words AS t1 INNER JOIN words2 AS t2 ON t1.name=t2.name;

INSERT INTO wordAssociation2 select t1.topicId, t2.idTo, t1.termFrequency*t3.wordCount, 0.0 from wordAssociation AS t1 INNER JOIN idtrans AS t2 on t1.wordId=t2.idFrom INNER JOIN topics AS t3 ON t1.topicId=t3.id;


#endif

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

char tolower( char c )
{
    if ( c >= 'A' && c <= 'Z' )
    {
        return 'a' + (c-'A');
    }
    else
    {
        return c;
    }
}

void extractWords( const std::string& text, std::map<std::string, int>& wordCount )
{
    char buf[8192];
    int bufIndex = 0;
    bool inWord = false;
    std::stringstream w;
    for ( size_t i = 0; i < text.size(); ++i )
    {
        char c = text[i];
        if ( inWord )
        {
            if ( c == ' ' )
            {
                buf[bufIndex] = '\0';
                wordCount[buf] += 1;
                bufIndex = 0;
                inWord = false;
            }
            else
            {
                buf[bufIndex++] = tolower(c);
            }
        }
        else
        {
            if ( c != ' ' )
            {
                inWord = true;
                buf[bufIndex++] = tolower(c);
            }
        }
    }
  
    if ( bufIndex > 0 )
    {
        buf[bufIndex] = '\0';
        wordCount[buf] += 1;
    }
}

void run()
{
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "../../data/WikipediaRawText.sqlite3" ) );
    
    boost::scoped_ptr<ise::sql::DbConnection> dbout( ise::sql::newSqliteConnection( "process.sqlite3" ) );
    dbout->execute( "CREATE TABLE topics( id INTEGER, title TEXT, wordCount INTEGER )" );
    dbout->execute( "CREATE TABLE words( id INTEGER, name TEXT, parentTopicCount INTEGER )" );
    dbout->execute( "CREATE TABLE wordAssociation( wordId INTEGER, topicId INTEGER, wordWeight REAL, tfidf REAL )" );
    
    //std::map<std::string, int> wordInTopicCount;
    //std::map<std::string, int> numTopicsWordSeenIn;
    // String to wordId, numTopicsWordSeenIn
    
    
    dbout->execute( "BEGIN" );
    std::map<std::string, boost::tuple<int, int> > wordDetails;
    {
        boost::scoped_ptr<ise::sql::PreparedStatement> addTopic( dbout->preparedStatement( "INSERT INTO topics VALUES( ?, ?, ? )" ) );
        boost::scoped_ptr<ise::sql::PreparedStatement> addWordAssoc( dbout->preparedStatement( "INSERT INTO wordAssociation VALUES( ?, ?, ?, 0.0 )" ) );
        
        int nextWordId = 0;
        size_t count = 0;
        size_t skipped = 0;
        boost::scoped_ptr<ise::sql::DbResultSet> rs( db->select( "SELECT id, title, body FROM topics" ) );
        while(true)
        {
            int topicId = count;
            boost::tuple<int, std::string, std::string> t;
            ise::sql::populateRowTuple( *rs, t );
            
            int id = t.get<0>();
            std::string& title = t.get<1>();
            std::string& body = t.get<2>();
            
            //std::cout << title << std::endl;
            
            int wordsInTopic = 0;
            {
                std::map<std::string, int> wordCount;
                extractWords( body, wordCount );
                
                for ( std::map<std::string, int>::iterator it = wordCount.begin(); it != wordCount.end(); ++it )
                {
                    if ( wordDetails.find( it->first ) == wordDetails.end() )
                    {
                        wordDetails.insert( std::make_pair( it->first, boost::make_tuple( nextWordId++, 0 ) ) );
                    }
                    wordDetails[it->first].get<1>() += 1;
                    wordsInTopic += it->second;
                }
                if ( wordsInTopic < 200 )
                {
                    skipped++;
                    continue;
                }
                
                for ( std::map<std::string, int>::iterator it = wordCount.begin(); it != wordCount.end(); ++it )
                {
                    
                    int wordId = wordDetails[it->first].get<0>();
                    int wordCount = it->second;
                    addWordAssoc->execute( boost::make_tuple( wordId, topicId, static_cast<float>( wordCount ) / static_cast<float>( wordsInTopic ) ) );
                }
            }
            
            addTopic->execute( boost::make_tuple( topicId, title, wordsInTopic ) );

            if ( ((++count) % 1000) == 0 ) std::cout << count << " : " << t.get<0>() << ", " << t.get<1>() << ", " << wordDetails.size() << ", skipped: " << skipped << std::endl;
            if ( !rs->advance() ) break;
        }
    }
    std::cout << "  Committing" << std::endl;
    dbout->execute( "COMMIT" );
    
    dbout->execute( "BEGIN" );
    {
        std::map<std::string, boost::tuple<int, int> >::iterator it;
        boost::scoped_ptr<ise::sql::PreparedStatement> addWord( dbout->preparedStatement( "INSERT INTO words VALUES( ?, ?, ? )" ) );
        for ( it = wordDetails.begin(); it != wordDetails.end(); ++it )
        {
            const std::string& word = it->first;
            int wordId = it->second.get<0>();
            int numTopicsWordSeenIn = it->second.get<1>();
            addWord->execute( boost::make_tuple( wordId, word, numTopicsWordSeenIn ) );
        }
    }
    std::cout << "  Committing" << std::endl;
    dbout->execute( "COMMIT" );

#if 0
    dbout->execute( "BEGIN" );
    
    
    
    std::cout << "Loading topics" << std::endl;
    std::map<int, Topic> topics;
    {
        boost::scoped_ptr<ise::sql::PreparedStatement> p( dbout->preparedStatement( "INSERT INTO topics VALUES( ?, ?, ? )" ) );
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
                p->execute( boost::make_tuple( topicId, text, wordCount ) );
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
        boost::scoped_ptr<ise::sql::PreparedStatement> p( dbout->preparedStatement( "INSERT INTO words VALUES( ?, ?, ? )" ) );
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
                    p->execute( boost::make_tuple( wordId, text, numParentTopics ) );
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
#endif
}

void run2()
{
    
    boost::scoped_ptr<ise::sql::DbConnection> db( ise::sql::newSqliteConnection( "./new.sqlite3" ) );
    
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
        //run2();
        run();
    }
    catch ( ise::exceptions::Generic& e )
    {   
        std::cout << e.str() << std::endl;
    }
    
    return 0;
}
