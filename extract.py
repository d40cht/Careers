import cProfile
import re

specialCharRe = re.compile('&[a-zA-Z\-0-9]+;')
wordRe = re.compile('[0-9a-zA-Z].+')
cssMarkupRe = re.compile('([a-zA-Z\-]+[ ]*\=[ ]*\"[^\"]+\")')
htmlTagRe = re.compile('<[^>]+>' )
templateParamRe = re.compile('{{{[^}]+}}}')
templateRe = re.compile('{{[^}]+}}')
externalLinkRe = re.compile('\[[^\]]+\]')

linkRe = re.compile( '\[\[([^\]]+)\]\]' )

def wikiStrip( wikiText ):
    newd = ''
    if 0:
        for l in wikiText.split('\n'):
            l = l.replace('[[', ' ')
            l = l.replace(']]', ' ' )
            l = re.sub( externalLinkRe, ' ', l )
            l = re.sub( specialCharRe, ' ', l )
            l = re.sub( templateParamRe, ' ', l )
            l = re.sub( templateRe, ' ', l )
            l = re.sub( cssMarkupRe, ' ', l )
            l = re.sub( htmlTagRe, ' ', l )
            newd += l

    else:
        links = re.findall( linkRe, wikiText )
        l = wikiText
        l = l.replace('[[', ' ')
        l = l.replace(']]', ' ' )
        l = re.sub( externalLinkRe, ' ', l )
        l = re.sub( specialCharRe, ' ', l )
        l = re.sub( templateParamRe, ' ', l )
        l = re.sub( templateRe, ' ', l )
        l = re.sub( cssMarkupRe, ' ', l )
        l = re.sub( htmlTagRe, ' ', l )
        newd = l
    
        	
    removeList='|{}[]@#~?/<>!:;"=%_*&,-\'().'

    for c in removeList:
        newd = newd.replace(c, ' ')
    
    #return newd
        
    #words = filter( lambda x: x != '' and wordRe.match(x) != None, [v.strip() for v in newd.split(' ')] )

    words = filter( lambda x: x != '', [v.strip() for v in newd.split(' ')] )

    return links, ' '.join(words)
 
def go():
    data = open( 'test.wmu', 'r' ).read()
    o = open( 'test.txt', 'w' )
    for i in range( 100 ):
        o.write( wikiStrip(data) )

if __name__ == '__main__':
    cProfile.run('go()')
