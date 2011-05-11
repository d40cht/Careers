package org.seacourt.berkeleydb

import java.io.File

import com.sleepycat.je.{Environment => SleepyEnvironment, EnvironmentConfig, Database, DatabaseConfig, DatabaseEntry, LockMode, OperationStatus, Transaction}

class Environment( val envPath : File, val allowCreate : Boolean )
{
    val transaction : Transaction = null
    val envConfig = new EnvironmentConfig()
    envConfig.setAllowCreate( allowCreate )
    if ( allowCreate && !envPath.exists() ) envPath.mkdir()
    val env = new SleepyEnvironment( envPath, envConfig )
    
    class Database( val fileName : String, val allowCreate : Boolean )
    {
        val dbConfig = new DatabaseConfig()
        dbConfig.setAllowCreate( allowCreate )
        val db = env.openDatabase( transaction, "db", dbConfig )
        
        def put( key : String, value : String )
        {
            val theKey = new DatabaseEntry( key.getBytes("UTF-8") )
            val theValue = new DatabaseEntry( value.getBytes("UTF-8") )
            db.put(transaction, theKey, theValue)
        }
        
        def get( key : String ) : Option[String] =
        {
            val theKey = new DatabaseEntry( key.getBytes("UTF-8") )
            val theValue = new DatabaseEntry()
         
            if ( db.get(transaction, theKey, theValue, LockMode.DEFAULT) == OperationStatus.SUCCESS )
            {
                Some( new String( theValue.getData, "UTF-8" ) )
            }
            else
            {
                None
            }
        }
        
        def close()
        {
            db.close()
        }
    }
    
    def openDb( name : String, allowCreate : Boolean ) =
    {
        new Database( name, allowCreate )
    }
    
    def deleteDb( name : String )
    {
        env.removeDatabase( transaction, name )
    }
    
    def close()
    {
        env.close()
    }
}






