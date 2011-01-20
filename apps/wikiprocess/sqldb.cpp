#include "sqldb.hpp"

#include <sqlite3.h>
#include <postgresql/libpq-fe.h>

#include <boost/lexical_cast.hpp>

#include "iseExceptions.hpp"


namespace ise { namespace sql {

class SqliteResultSet : public DbResultSet
{
public:
	SqliteResultSet( sqlite3* conn, const std::string& query );
	~SqliteResultSet();
	virtual size_t size();
	virtual bool advance();
	
	virtual void populateField( double& value, size_t colIndex );
	virtual void populateField( int& value, size_t colIndex );
	virtual void populateField( std::string& value, size_t colIndex );

private:
	sqlite3_stmt*	m_stmt;
};

class SqlitePreparedStatement : public PreparedStatement
{
public:
    SqlitePreparedStatement( sqlite3* conn,const std::string& query );
    ~SqlitePreparedStatement();

private:
    virtual void _reset();
    virtual void _exec();
    virtual void bindArg( int index, const double& value );
    virtual void bindArg( int index, const int& value );
    virtual void bindArg( int index, const std::string& value );
    
private:
    sqlite3*            m_conn;
    sqlite3_stmt*	    m_stmt;
};
	
	
class SqliteConnection : public DbConnection
{
public:
	SqliteConnection( const std::string& fileName );
	~SqliteConnection();
	
	virtual DbResultSet *select( const std::string& query );
	virtual void execute( const std::string& query );
	virtual PreparedStatement* preparedStatement( const std::string& query );
	
private:
	sqlite3*	m_conn;
};


class PostgresResultSet : public DbResultSet
{
public:
	PostgresResultSet( PGresult* res );
	~PostgresResultSet();
	virtual size_t size();
	virtual bool advance();
	
	// TODO: Could be much faster operating in binary mode with a set of template
	// specialisations for each data type (needs to validate the column type)
	/*template<typename T>
	void populateField( T& value, size_t colIndex )
	{
		value = boost::lexical_cast<T>( PQgetvalue( m_res, m_currRow, colIndex ) );
	}*/
	
	virtual void populateField( double& value, size_t colIndex ) { value = boost::lexical_cast<double>( PQgetvalue( m_res, m_currRow, colIndex ) ); }
	virtual void populateField( int& value, size_t colIndex ) { value = boost::lexical_cast<int>( PQgetvalue( m_res, m_currRow, colIndex ) ); }
	virtual void populateField( std::string& value, size_t colIndex ) { value = boost::lexical_cast<std::string>( PQgetvalue( m_res, m_currRow, colIndex ) ); }
	
private:
	PGresult*	m_res;
	size_t		m_currRow;
};

class PostgresPreparedStatement : public PreparedStatement
{
public:
    PostgresPreparedStatement( PGconn* conn, const std::string& query );
    ~PostgresPreparedStatement();

private:
    virtual void _reset();
    virtual void _exec();
    virtual void bindArg( int index, const double& value );
    virtual void bindArg( int index, const int& value );
    virtual void bindArg( int index, const std::string& value );
    
private:
    PGconn*   m_conn;
};


class PostgresConnection : public DbConnection
{
public:
	PostgresConnection( const std::string& connInfo );
	~PostgresConnection();
	
	virtual DbResultSet* select( const std::string& query );
	virtual void execute( const std::string& query );
	virtual PreparedStatement* preparedStatement( const std::string& query );
	
private:
	PGconn*	m_conn;
};

DbConnection* newSqliteConnection( const std::string& fileName )
{
	return new SqliteConnection( fileName );
}

DbConnection* newPostgresConnection( const std::string& connectionString )
{
	return new PostgresConnection( connectionString );
}


SqliteResultSet::SqliteResultSet( sqlite3* conn, const std::string& query ) : m_stmt( NULL )
{
	int res = sqlite3_prepare_v2( conn, query.c_str(), query.size(), &m_stmt, NULL );
	
	if ( res != SQLITE_OK )
	{
		ISE_THROW( ise::exceptions::Generic )( std::string( "Query failed: " ) + query + ", " + std::string(sqlite3_errmsg(conn)) );
	}
	
	sqlite3_step( m_stmt );
}

SqliteResultSet::~SqliteResultSet()
{
	sqlite3_finalize( m_stmt );
}

size_t SqliteResultSet::size()
{
	return sqlite3_data_count( m_stmt );
}

bool SqliteResultSet::advance()
{
	return sqlite3_step( m_stmt ) == SQLITE_ROW;
}

void SqliteResultSet::populateField( double& value, size_t colIndex )
{
	value = sqlite3_column_double( m_stmt, colIndex );
}

void SqliteResultSet::populateField( int& value, size_t colIndex )
{
	value = sqlite3_column_int( m_stmt, colIndex );
}

void SqliteResultSet::populateField( std::string& value, size_t colIndex )
{
	value = std::string( reinterpret_cast<const char*>( sqlite3_column_text( m_stmt, colIndex ) ), sqlite3_column_bytes( m_stmt, colIndex ) );
}

SqlitePreparedStatement::SqlitePreparedStatement( sqlite3* conn, const std::string& query ) : PreparedStatement(query), m_conn(conn)
{
    int res = sqlite3_prepare_v2( m_conn, query.c_str(), query.size(), &m_stmt, NULL );
	
	if ( res != SQLITE_OK )
	{
		ISE_THROW( ise::exceptions::Generic )( std::string( "Query failed: " ) + query + ", " + std::string(sqlite3_errmsg(conn)) );
	}
}

void SqlitePreparedStatement::_reset()
{
    sqlite3_reset( m_stmt );
}


void SqlitePreparedStatement::_exec()
{
    sqlite3_step( m_stmt );
}

void SqlitePreparedStatement::bindArg( int index, const double& value )
{
    sqlite3_bind_double( m_stmt, index+1, value );
}

void SqlitePreparedStatement::bindArg( int index, const int& value )
{
    sqlite3_bind_int( m_stmt, index+1, value );
}

void SqlitePreparedStatement::bindArg( int index, const std::string& value )
{
    sqlite3_bind_text( m_stmt, index+1, value.c_str(), value.length(), SQLITE_STATIC );
}


SqliteConnection::SqliteConnection( const std::string& fileName )
{
	int res = sqlite3_open( fileName.c_str(), &m_conn );
	
	if ( res != SQLITE_OK )
	{
		ISE_THROW( ise::exceptions::Generic )( std::string( "Could not open sqlite db: " ) + fileName + ", " + std::string(sqlite3_errmsg(m_conn)) );
	}
}

SqliteConnection::~SqliteConnection()
{
	sqlite3_close( m_conn );
}

DbResultSet* SqliteConnection::select( const std::string& query )
{
	return new SqliteResultSet( m_conn, query );
}

void SqliteConnection::execute( const std::string& query )
{
	SqliteResultSet temp( m_conn, query );
}

PreparedStatement* SqliteConnection::preparedStatement( const std::string& query )
{
    return new SqlitePreparedStatement( m_conn, query );
}

PostgresResultSet::PostgresResultSet( PGresult* res ) : m_res( res ), m_currRow(0)
{
}

PostgresResultSet::~PostgresResultSet()
{
	PQclear( m_res );
}

size_t PostgresResultSet::size()
{
	return (size_t) PQntuples( m_res );
}

bool PostgresResultSet::advance()
{
	m_currRow++;
	return m_currRow < size();
}


PostgresPreparedStatement::PostgresPreparedStatement( PGconn* conn, const std::string& query ) : PreparedStatement(query), m_conn(conn)
{
}

PostgresPreparedStatement::~PostgresPreparedStatement()
{
}

void PostgresPreparedStatement::_reset()
{
}


void PostgresPreparedStatement::_exec()
{
}

void PostgresPreparedStatement::bindArg( int /*index*/, const double& /*value*/ )
{
}

void PostgresPreparedStatement::bindArg( int /*index*/, const int& /*value*/ )
{
}

void PostgresPreparedStatement::bindArg( int /*index*/, const std::string& /*value*/ )
{
}

PostgresConnection::PostgresConnection( const std::string& connInfo )
{
	m_conn = PQconnectdb( connInfo.c_str() );
	
	if ( PQstatus( m_conn ) != CONNECTION_OK )
	{
		ISE_THROW( ise::exceptions::Generic )( std::string( "Could not connect to postgres db: " ) + connInfo + ", " + std::string(PQerrorMessage(m_conn)) );
	}
}

PostgresConnection::~PostgresConnection()
{
	PQfinish( m_conn );
}

DbResultSet* PostgresConnection::select( const std::string& query )
{
	PGresult* res = PQexec( m_conn, query.c_str() );
	
	if ( PQresultStatus(res) != PGRES_TUPLES_OK )
	{
		PQclear( res );
		ISE_THROW( ise::exceptions::Generic )( std::string( "Query failed: " ) + query + ", " + std::string(PQerrorMessage(m_conn)) );
	}
	
	return new PostgresResultSet( res );
}

void PostgresConnection::execute( const std::string& query )
{
	PGresult* res = PQexec( m_conn, query.c_str() );
	
	if ( PQresultStatus(res) != PGRES_COMMAND_OK )
	{
		PQclear( res );
		ISE_THROW( ise::exceptions::Generic )( std::string( "Query failed: " ) + query + ", " + std::string(PQerrorMessage(m_conn)) );
	}
}

PreparedStatement* PostgresConnection::preparedStatement( const std::string& query )
{
    return new PostgresPreparedStatement( m_conn, query );
}

}}

