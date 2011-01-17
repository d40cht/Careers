#pragma once

#include <boost/tuple/tuple.hpp>

#include <string>

namespace ise { namespace sql {

template<typename ResultSetType, typename T>
void populateRowTuple_shim( size_t colIndex, ResultSetType& resultSet, boost::tuples::cons<T, boost::tuples::null_type>& tuple )
{
	resultSet.populateField( tuple.head, colIndex );
}

template<typename ResultSetType, typename T1, typename T2>
void populateRowTuple_shim( size_t colIndex, ResultSetType& resultSet, boost::tuples::cons<T1, T2>& tuple )
{
	resultSet.populateField( tuple.head, colIndex );
	populateRowTuple_shim( colIndex+1, resultSet, tuple.tail );
}



template<typename ResultSetType, typename T>
void populateRowTuple( ResultSetType& resultSet, T& row )
{
	populateRowTuple_shim( 0, resultSet, row );
}

template<typename ResultSetType, template <typename, typename> class Container, class T>
void populateTupleContainer( ResultSetType& resultSet, Container<T, std::allocator<T> >& c )
{
	bool done = false;
	while ( !done )
	{
		T tuple;
		populateRowTuple( resultSet, tuple );
		c.push_back( tuple );
		done = !resultSet.advance();
	}
}

class DbResultSet
{
public:
	virtual ~DbResultSet() {}
	
	virtual size_t size() = 0;
	virtual bool advance() = 0;
	
	virtual void populateField( double& value, size_t colIndex ) = 0;
	virtual void populateField( int& value, size_t colIndex ) = 0;
	virtual void populateField( std::string& value, size_t colIndex ) = 0;
};

class DbConnection
{
public:
	virtual ~DbConnection() {}
	
	virtual DbResultSet* select( const std::string& query ) = 0;
	virtual void execute( const std::string& query ) = 0;
};

DbConnection* newSqliteConnection( const std::string& filename );
DbConnection* newPostgresConnection( const std::string& connectionString );

}}





