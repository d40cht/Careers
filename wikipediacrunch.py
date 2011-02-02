import os
import re
import bz2
import math
import string
import extract
import sqlite3
import datetime

import cProfile


# Stem words using nltk?
# Deal with templates properly (mod mwlib?)

from xml.etree.cElementTree import ElementTree, iterparse


def createDb( conn ):
    conn.execute( 'CREATE TABLE rawTopics( id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, raw TEXT )' )
    conn.execute( 'CREATE TABLE processedTopics( id, title TEXT, text TEXT )' )
    conn.execute( 'CREATE TABLE links( fromId INTEGER, toId INTEGER, linkText TEXT )' )
    
def allindices(string, sub):
    listindex = []
    offset = 0
    i = string.find(sub, offset)
    while i >= 0:
        listindex.append(i)
        i = string.find(sub, i + len(sub)) 
    return listindex
    
def extractTemplates( text ):
    allOpens = allindices(text, '{{')
    allCloses = allindices(text, '}}')
    if allOpens == [] or allCloses == []:
        return text, []
        
    allT = sorted([(v, '{{') for v in allOpens] + [(v, '}}') for v in allCloses])

    templates = []
    transformedText = ''
    nesting = 0
    nestingOpen = 0
    lastIndex = 0
    for v, t in allT:
        #print v, t
        if t == '{{':
            if nesting == 0:
                nestingOpen = v+2
                transformedText += text[lastIndex:v]
            nesting += 1
        elif t == '}}':
            nesting -= 1
            if nesting == 0:
                lastIndex = v + 2
                templates.append( text[nestingOpen:v] )
    transformedText += text[lastIndex:]
    return transformedText, templates
    
def ignoreTemplates(templates):
    for t in templates:
        if t.startswith('POV') or t.startswith('advert') or t.startswith('trivia'):
            return True
    return False
    
disallowedTitlePrefixes = ['Portal:', 'User:', 'File:', 'Wikipedia:', 'Template:']
def addPage( conn, title, rawtext ):
    if reduce( lambda x, y: x and (not title.startswith(y)), disallowedTitlePrefixes, True ):
        text, templates = extractTemplates(rawtext)
        
        if ignoreTemplates( templates ):
            print 'Ignoring ', title, ' because templates suggest low quality.'
        else:
            rowId = conn.execute( 'INSERT INTO rawTopics VALUES(NULL, ?, ?)', (title, rawtext) ).lastrowid
    else:
        print '  skipping as title prefix suggests not article.'

wikipediaExportNs = 'http://www.mediawiki.org/xml/export-0.4/'
def expTag(s):
    return '{%s}%s' % (wikipediaExportNs, s)

def extractRawData(fileName, dbconn):
    commitInterval = 10000
    if 1:
        count = 0
        fileStream = bz2.BZ2File( fileName, 'r' )
        for event, element in iterparse( fileStream ):
            if element.tag == expTag('page'):
                title = element.find(expTag('title')).text

                print count, title
                
                textElement = element.find(expTag('revision')).find(expTag('text'))
                if textElement != None and textElement.text != None:
                    text = textElement.text.strip()
                    addPage( conn, title, text )
                    
                    if (count % commitInterval) == 0:
                        print '************* Committing changes **************'
                        conn.commit()

                count += 1

                element.clear()
        
    conn.commit()


def processLinks( conn, titleIdDict, fromId, links ):
    linksProcessed = []
    for link in links:
        if link.find('[') == -1 and link.find(']') == -1:
            fields = link.split('|')
            if len(fields) == 1:
                linkTitle = fields[0].strip()
                linkText = ''
            elif len(fields) == 2:
                linkTitle = fields[0].strip()
                linkText = fields[1].strip()
            else:
                print 'Malformed link', link
                continue
            if linkTitle in titleIdDict:
                toId = titleIdDict[linkTitle]
                linksProcessed.append( (fromId, toId, linkText) )
            else:
                print 'Missing link: %s ^ %s' % (linkTitle, linkText)
                
    return linksProcessed
                
            

def buildRawTextArticles( conn, titleIdDict ):
    print 'Building index on rawTopics'
    conn.execute( 'CREATE INDEX i1 ON rawTopics(title)' )
    conn.commit()
        
    print 'Processing wikimarkup to generate plain text and wiki links'
    start = datetime.datetime.now()
    r = conn.execute( 'SELECT id, title, raw FROM rawTopics' )
    c = 0
    for rowId, title, rawText in r:
        links, text = extract.wikiStrip( rawText )
        print links
        conn.execute( 'INSERT INTO processedTopics VALUES(?, ?, ?)', (rowId, title, text) )
        #processLinks( conn, titleIdDict, rowId, links )
        c += 1
        if ((c % 1000) == 0):
            now = datetime.datetime.now()
            hoursDiff = float( (now-start).seconds) / (3600.0)
            print ':: %dk articles (%dk per hour)' % ((c/1000), (float(c) / (1000.0 * hoursDiff)))

        if ((c % 10000) == 0):
            print 'Committing'
            connOut.commit()
            
#select count(title) from rawTopics where title like '%disambiguation%' limit 10; (158,191)
#select count(raw) from rawTopics where raw like '#redirect %'; (4,273,222)
#select count(title) from rawTopics where title like 'table of%' or title like 'list of%'; (122,800)


                
if __name__ == '__main__':
    fileName = './data/enwiki-latest-pages-articles.xml.bz2'
    dbFileName = 'rawData.sqlite3'

    existsAlready = os.path.exists( dbFileName )
    conn = sqlite3.connect( dbFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    if not existsAlready:
        createDb( conn )

    extractRawData(fileName, conn)
    #processRawData(conn, titleIdDict)
    
