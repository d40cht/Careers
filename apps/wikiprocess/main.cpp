#include "sqldb.hpp"

#include <map>
#include <vector>
#include <algorithm>
#include <iostream>

#include <cmath>

#include <boost/scoped_ptr.hpp>
#include <boost/tuple/tuple_comparison.hpp>

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
            
            int topicId = t.get<0>();
            int wordCount = t.get<1>();

            if ( wordCount > 30 )
            {            
                topics.insert( std::make_pair( topicId, Topic(wordCount) ) );
    	        if ( ((++count) % 100000) == 0 ) std::cout << count << std::endl;
            }
            if ( !rs->advance() ) break;
        }
    }
    int numTopics = topics.size();
    std::cout << "  " << numTopics << std::endl;
    
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
            int numParentTopics = t.get<1>();
            
            if ( numParentTopics > 2 )
            {
                words.insert( std::make_pair( wordId, Word( numTopics, numParentTopics ) ) );

	            if ( ((++count) % 100000) == 0 ) std::cout << count << " : " << t.get<0>() << ", " << t.get<1>() << std::endl;
            }
            if ( !rs->advance() ) break;
        }
    }
    std::cout << "  " << words.size() << std::endl;
    
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
		    
		    float wordInverseDocFrequency = words.find(wordId)->second.m_inverseDocFrequency;
		    int wordsInTopic = topics.find(topicId)->second.m_wordCount;
		    
		    float wordImportanceInTopic = (float) wordCountInTopic / (float) wordsInTopic;
		    float tfIdf = wordImportanceInTopic * wordInverseDocFrequency;
		    
		    //wordAssocs.push_back( boost::make_tuple( t.get<1>(), t.get<0>(), tfIdf ) );
		    std::multimap<float, int>& wordMap = wordAssocs[wordId];
		    
		    wordMap.insert( std::make_pair( tfIdf, topicId ) );
		    
		    // Keep only the highest importance topics per word
		    if ( wordMap.size() > 200 )
		    {
		        wordMap.erase( wordMap.begin() );
		    }
		
		    if ( ((++count) % 1000000) == 0 ) std::cout << count << std::endl;
		
		    if ( !rs->advance() )
		    {
		        break;
            }
	    }
    }
    
    std::cout << "Counting cut down topic map" << std::endl;
    int limitedCount = 0;
    for ( std::map<int, std::multimap<float, int> >::iterator it = wordAssocs.begin(); it != wordAssocs.end(); ++it )
    {
        limitedCount += it->second.size();
    }
    
    std::cout << "  " << limitedCount << " topics associations" << std::endl;
}
