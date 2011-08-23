import os
import sqlite3
import urllib
import urllib2
import simplejson

def createDb( conn ):
    cur = conn.cursor()
    cur.execute( 'CREATE TABLE sectors( id INTEGER PRIMARY KEY AUTOINCREMENT, searchQuery TEXT )' )
    cur.execute( 'CREATE TABLE sectorQueryResults( id INTEGER PRIMARY KEY AUTOINCREMENT, sectorId INTEGER, url TEXT, title TEXT, abstract TEXT )' )
    cur.execute( 'CREATE TABLE cvText( id INTEGER PRIMARY KEY AUTOINCREMENT, sectorQueryResultId INTEGER, contents TEXT )' )
    cur.execute( 'CREATE INDEX i1 ON sectorQueryResults (sectorId)')
    cur.execute( 'CREATE INDEX i2 ON sectorQueryResults (url)')
    cur.execute( 'CREATE INDEX i3 ON cvText(sectorQueryResultId)')
    conn.commit()

def yahooSearch( query, numResults=20 ):
    urlstring = 'select url, title, abstract from search.web(%d) where query="%s" AND region="UK"' % (numResults, query)
    url = urllib.quote( urlstring )
    result = urllib2.urlopen('http://query.yahooapis.com/v1/public/yql?q=%s&format=json' % url).read()
    return simplejson.loads(result)


dbfile = 'cvscrape.sqlite3'
existsAlready = os.path.exists( dbfile )
conn = sqlite3.connect( dbfile, detect_types=sqlite3.PARSE_DECLTYPES )
if not existsAlready:
    createDb( conn )

c = conn.cursor()
for sectorId, sectorName, resCount in list(c.execute('SELECT t1.id, t1.searchQuery, count(t2.url) FROM sectors AS t1 LEFT JOIN sectorQueryResults AS t2 ON t1.id=t2.sectorId GROUP BY t1.id' )):
    if resCount != 0:
        print 'Sector %s has %d results' % (sectorName, resCount)
    else:
        print 'Querying yahoo for %s' % sectorName
        res = yahooSearch( 'Curriculum Vitae British %s filetype:pdf' % sectorName, numResults=0)['query']
        print '  got %s results' % res['count']
        insertedCount = 0
        for row in res['results']['result']:
            url = row['url']
            title = row['title']
            abstract = row['abstract']

            already = c.execute( "SELECT * FROM sectorQueryResults WHERE url=?", [url] ).fetchall()
            if len(already) == 0:
                c.execute( "INSERT INTO sectorQueryResults VALUES( NULL, ?, ?, ?, ? )", [sectorId, url, title, abstract] )
                insertedCount += 1
        print '  commiting %d unique results' % insertedCount
        conn.commit()

for urlId, url, textId in list(c.execute('SELECT t1.id, t1.url, t2.id FROM sectorQueryResults AS t1 LEFT JOIN cvText AS t2 ON t1.id=t2.sectorQueryResultId')):
    if textId == None:
        print 'Getting: %s' % url
        try:
            res = os.system( 'wget %s -O temp.pdf -T 30 -t 3' % url )
            if res != 0: raise IOError('Could not get data' )
            res = os.system( '/usr/bin/pdftotext -nopgbrk temp.pdf' )
            if res != 0: raise IOError('Could not parse pdf' )
            text = open( 'temp.txt', 'r' ).read()

            textSimple = ''
            for char in text:
                if ord(char) < 128:
                    textSimple += char

            print 'Inserting text into db (%d)' % len(textSimple)
            c.execute( 'INSERT INTO cvText VALUES(NULL, ?, ?)', [urlId, textSimple] )
            conn.commit()
        except IOError:
            print '  IO error. Continuing'
        

