import re

linkREtxt = '\[\[(([^\:]+)\:)?([^\]\#\|]+)(\#([^\]\|]+))?(\|([^\]]+))?'
linkRE = re.compile( linkREtxt )

def linkMatch( text ):
    m = linkRE.match( text )
    if m:
        namespace = m.group(2)
        linkPage = m.group(3)
        pageAnchor = m.group(5)
        alternateText = m.group(7)
        
        #print namespace, linkPage, pageAnchor, alternateText
        return (namespace, linkPage, pageAnchor, alternateText)

    return None
    
assert( linkMatch( '[[dasdas]]' ) == (None, 'dasdas', None, None) )
assert( linkMatch( '[[dasdas#dasda]]' ) == (None, 'dasdas', 'dasda', None) )
assert( linkMatch( '[[dasdas#dasda|dasdh]]' ) == (None, 'dasdas', 'dasda', 'dasdh') )
assert( linkMatch( '[[dasdas|dasdh]]' ) == (None, 'dasdas', None, 'dasdh') )
assert( linkMatch( '[[category:dasdas]]' ) == ('category', 'dasdas', None, None) )
assert( linkMatch( '[[category:dasdas#dasda]]' ) == ('category', 'dasdas', 'dasda', None) )
assert( linkMatch( '[[category:dasdas#dasda|dasdh]]' ) == ('category', 'dasdas', 'dasda', 'dasdh') )
assert( linkMatch( '[[category:dasdas|dasdh]]' ) == ('category', 'dasdas', None, 'dasdh') )

redirectRE = re.compile('\#redirect[ ]*%s([ ]*\{\{([^\}]+)\}\})?' % linkREtxt)

def redirectMatch( text ):
    m = redirectRE.match( text )
    if m:
        namespace = m.group(2)
        linkPage = m.group(3)
        pageAnchor = m.group(5)
        alternateText = m.group(7)
        redirectReason = m.group(8)
        return (namespace, linkPage, pageAnchor, alternateText, redirectReason)

# Process a redirect for alternate surface forms
def processRedirect( titleDict, pageTitle, pageText, processedConn ):
    rawText = pageText.replace( '\n', ' ' )
    m = redirectMatch( pageText )
    if m:
        redirectFrom = pageTitle
        redirectTo = m[2]
        redirectReason = m[4]
        if redirectReason == None or redirectReason not in uninterestingReasons:
            if redirectTo in titleDict:
                toId = titleDict[redirectTo]
                processedConn.execute( 'INSERT INTO alternateNames VALUES( ?, ? )', [redirectFrom, toId] )
                print 'Adding redirect alternate name', redirectFrom, toId

# Process links for alternate surface forms, for topic connectivity and for category membership
def processLinks( titleDict, pageId, links, processedConn ):
    for link in links:
        m = linkMatch( link )
        if m != None:
            namespace, linkPage, pageAnchor, alternateText = m
            if linkPage != None and pageAnchor == None and linkPage in titleDict:
                if namespace == None:
                    fullLink = linkPage
                else:
                    fullLink = '%s:%s' % (namespace, linkPage)
                if linkToId in titleDict:
                    linkToId = titleDict[fullLink]
                    # Surface form
                    if namespace == None and alternateText != None:
                        processedConn.execute( 'INSERT INTO alternateNames VALUES( ?, ? )', [alternateText, linkToId] )
                        print 'Adding link alternate name', alternateText, linkToId

                    # Topic connectivity
                    processedConn.execute( 'INSERT INTO topicLinks VALUES( ?, ? )', [pageId, linkToId] )
                    print 'Adding link from ', pageId, linkToId
                

