import os
import re
import bz2
import math
import string
import sqlite3
import datetime

import cProfile

import extract

# Stem words using nltk?
# Deal with templates properly (mod mwlib?)


def run2():
    dbFileName = 'rawData.sqlite3'
    dbOutFileName = 'text.sqlite3'
    conn = sqlite3.connect( dbFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    connOut = sqlite3.connect( dbOutFileName, detect_types=sqlite3.PARSE_DECLTYPES )
    
    start = datetime.datetime.now()
    r = conn.execute( 'SELECT id, title, raw FROM rawTopics' )
    c = 0
    for rowId, title, rawText in r:
        #text, templates = extractTemplates(rawText)
        #words = parsePage( text )
        text = extract.wikiStrip( rawText )
        connOut.execute( 'INSERT INTO topics VALUES(?, ?, ?)', (rowId, title, text) )
        c += 1
        if ((c % 1000) == 0):
            now = datetime.datetime.now()
            hoursDiff = float( (now-start).seconds) / (3600.0)
            print ':: %dk articles (%dk per hour)' % ((c/1000), (float(c) / (1000.0 * hoursDiff)))

        if ((c % 10000) == 0):
            print 'Committing'
            connOut.commit()


if __name__ == '__main__':
    run2()
    
