import os
import math
import sqlite3

from xml.etree.cElementTree import ElementTree, iterparse

def createDb( conn ):
    cur = conn.cursor()
    cur.execute( 'CREATE TABLE topics( id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, wordCount INTEGER )' )
    cur.execute( 'CREATE TABLE words( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, parentTopicCount INTEGER, inverseDocumentFrequency REAL )' )
    cur.execute( 'CREATE TABLE wordAssociation( topicId INTEGER, wordId INTEGER, termFrequency REAL, termWeight REAL )' )
    cur.execute( 'CREATE INDEX i1 ON words(name)' )
    cur.execute( 'CREATE INDEX i2 ON wordAssociation(wordId)' )
    cur.execute( 'CREATE INDEX i3 ON wordAssociation(wordId, topicId)' )
    conn.commit()
    
wordIds = {}

def getTopicsForWord( conn, word ):
    return conn.execute(
        "SELECT t2.title, t1.termWeight FROM wordAssociation AS t1 INNER JOIN topics AS t2 ON t1.topicId=t2.id "
        "INNER JOIN words AS t3 ON t1.wordId=t3.id WHERE t3.name='assistants' ORDER BY t1.termWeight DESC", [word] )
    
def addPage( conn, title, text ):
    if not title.startswith('Category:') and not title.startswith('Portal:') and not title.startswith('User:'):
        print 'Adding page: ', title
        words = []
        word = ''
        for c in text.lower():
            if c >= 'a' and c <= 'z':
                word += c
            else:
                if word != '':
                    words.append(word)
                    word = ''
        if word != '':
            words.append(word)

        cur = conn.cursor()
        cur.execute( 'INSERT INTO topics VALUES(NULL, ?, ?)', (title, len(words)) )
        topicId = cur.lastrowid
        topicWordCount = {}
        for word in words:
            if word not in wordIds:
                cur.execute( 'INSERT INTO words VALUES( NULL, ?, 0, 0 )', [word] )
                wordIds[word] = cur.lastrowid
            wordId = wordIds[word]
            if word not in topicWordCount:
                topicWordCount[wordId] = 0
            topicWordCount[wordId] += 1

        numWords = len(words)
        for wordId, count in topicWordCount.items():
            termFrequency = float(count) / float(numWords)
            cur.execute( 'UPDATE words SET parentTopicCount=parentTopicCount+1 WHERE id=?', [wordId] )
            cur.execute( 'INSERT INTO wordAssociation VALUES(?, ?, ?, 0)', [topicId, wordId, termFrequency] )
            
def buildWeights( conn ):
    print 'Building inverse document frequencies'
    count = conn.execute( 'SELECT COUNT(*) FROM topics' ).fetchone()[0]
    res = conn.execute( 'UPDATE words SET inverseDocumentFrequency=log(? / parentTopicCount)', [count] )
    conn.commit()

    print 'Building word weightings'
    res = conn.execute( 'UPDATE wordAssociation SET termWeight=(termFrequency * (SELECT inverseDocumentFrequency FROM words WHERE id=wordId))' )
    conn.commit()
    print 'Complete'

def run():
    fileName = 'data/Wikipedia-20110112203017.xml'
    dbFileName = 'parsed.sqlite3'

    existsAlready = os.path.exists( dbFileName )
    conn = sqlite3.connect( dbFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    if not existsAlready:
        createDb( conn )
    conn.create_function( 'log', 1, math.log )

    if 1:
        for event, element in iterparse( fileName ):
            if element.tag == 'page':
                title = element.find('title').text
                
                textElement = element.find('revision').find('text')
                if textElement != None:
                    text = textElement.text
                    addPage( conn, title, text )
                element.clear()
        conn.commit()
    
    buildWeights( conn )
                
if __name__ == '__main__':
    run()
