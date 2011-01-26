import os
import re
import bz2
import math
import string
import sqlite3

import cProfile

import extract

# Stem words using nltk?
# Deal with templates properly (mod mwlib?)

if 0:
    #from xml.etree.cElementTree import ElementTree, iterparse
    import mwlib
    from mwlib.refine import compat
    from mwlib.parser import nodes


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
        #text, templates = extractTemplates(rawText)
        #words = parsePage( text )
        text = extract.wikiStrip( rawText )
        c += 1
        if ((c % 100) == 0): print ':: ', c


if __name__ == '__main__':
    run2()
    
