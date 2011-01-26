import os
import re
import bz2
import math
import mwlib
import string
import sqlite3

import cProfile


# Stem words using nltk?
# Deal with templates properly (mod mwlib?)

from xml.etree.cElementTree import ElementTree, iterparse
from mwlib.refine import compat
from mwlib.parser import nodes


#def createDb( conn ):
#    cur = conn.cursor()
#    cur.execute( 'CREATE TABLE topics( id INTEGER, title TEXT, wordCount INTEGER )' )
#    cur.execute( 'CREATE TABLE words( id INTEGER PRIMARY KEY, name TEXT, parentTopicCount INTEGER, inverseDocumentFrequency REAL )' )
#    cur.execute( 'CREATE TABLE wordAssociation( topicId INTEGER, wordId INTEGER, termFrequency REAL, termWeight REAL )' )
#    cur.execute( 'CREATE INDEX i1 ON words(name)' )
#    cur.execute( 'CREATE INDEX i4 ON topics(title)' )
#    conn.commit()

def createDb( conn ):
    conn.execute( 'CREATE TABLE rawTopics( id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, raw TEXT )' )
    
wordIds = {}
validWord = re.compile('[a-z][a-z0-9\+\-\#\/]+')

def mungeWords(words):
    filtered = []
    for w in words:
        w = w.strip('"\'.();:,?@<>/')
        if w.startswith('{{') and w.endswith('}}'):
            print 'Template: ', w
        elif validWord.match( w ) == None:
            pass
        else:
            filtered.append( w )
    return filtered

def parseTree(mwel):
    res = []
    if isinstance( mwel, nodes.Text ):
        res += [v.strip().lower() for v in mwel.asText().split(' ')]
    elif  isinstance( mwel, nodes.Article ) or isinstance( mwel, nodes.Section ) or isinstance( mwel, nodes.Item ) or isinstance( mwel, nodes.Style ) or isinstance( mwel, nodes.Node ):
        for v in mwel:
            res += parseTree(v)
    else:
        print 'Unexpected node type:', type(mwel)
        
    return res
    
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
 
    
def parsePage( text ):
    words = parseTree( compat.parse_txt(text, lang=None) )
    return mungeWords(words)
    
def ignoreTemplates(templates):
    for t in templates:
        if t.startswith('POV') or t.startswith('advert') or t.startswith('trivia'):
            return True
    return False
    
    
disallowedTitlePrefixes = ['Category:', 'Portal:', 'User:', 'File:', 'Wikipedia:', 'Template:']
def addPage( conn, title, rawtext ):
    if reduce( lambda x, y: x and (not title.startswith(y)), disallowedTitlePrefixes, True ):
        
        text, templates = extractTemplates(rawtext)
        
        if ignoreTemplates( templates ):
            print 'Ignoring ', title, ' because templates suggest low quality.'
        else:
            #words = parsePage( text )
            #conn.execute( 'INSERT INTO topics VALUES(NULL, ?, ?, ?)', (title, rawtext) )
            conn.execute( 'INSERT INTO rawTopics VALUES(NULL, ?, ?)', (title, rawtext) )
    else:
        print '  skipping as title prefix suggests not article.'
            


def run2():
    dbFileName = 'rawData.sqlite3'
    conn = sqlite3.connect( dbFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    
    r = conn.execute( 'SELECT id, title, raw FROM rawTopics' )
    c = 0
    for rowid, title, rawText in r:
        text, templates = extractTemplates(rawText)
        words = parsePage( text )
        c += 1
        if (c % 10 == 0): print ':: ', c


wikipediaExportNs = 'http://www.mediawiki.org/xml/export-0.4/'
def expTag(s):
    return '{%s}%s' % (wikipediaExportNs, s)

def run():
    fileName = '/home/alexw/enwiki-latest-pages-articles.xml.bz2'
    dbFileName = 'rawData.sqlite3'

    existsAlready = os.path.exists( dbFileName )
    conn = sqlite3.connect( dbFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    if not existsAlready:
        createDb( conn )

    titles = set( [v[0] for v in conn.execute('SELECT title FROM rawTopics')] )
    commitInterval = 10000
    if 1:
        count = 0
        fileStream = bz2.BZ2File( fileName, 'r' )
        for event, element in iterparse( fileStream ):
            if element.tag == expTag('page'):
                title = element.find(expTag('title')).text

                if not title in titles:
                    print count, title
                    
                    textElement = element.find(expTag('revision')).find(expTag('text'))
                    if textElement != None and textElement.text != None:
                        text = textElement.text
                        addPage( conn, title, text )
                        
                        if (count % commitInterval) == 0:
                            print '************* Committing changes **************'
                            conn.commit()
                else:
                    print 'Skipping ', title, count
                count += 1

                element.clear()
        
    conn.commit()
                
if __name__ == '__main__':
    run()
    #run2()
    
