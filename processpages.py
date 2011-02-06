linkREtxt = '\[\[(([^\:]+)\:)?([^\]\#]+)(\#[^\]^\|]+)?(\|([^\]]+))?'
linkRE = re.compile( linkREtxt )

def linkMatch( text ):
    m = linkRE.match( text )
    if m:
        namespace = m.group(2)
        linkPage = m.group(3)
        pageAnchor = m.group(4)
        alternateText = m.group(6)
        return (namespace, linkPage, pageAnchor, alternateText)

    return None
    
assert( linkMatch( '[[dasdas]]' ), (None, 'dasdas', None, None) )
assert( linkMatch( '[[dasdas#dasda]]' ), (None, 'dasdas', 'dasda', None) )
assert( linkMatch( '[[dasdas#dasda|dasdh]]' ), (None, 'dasdas', 'dasda', 'dasdh') )
assert( linkMatch( '[[dasdas|dasdh]]' ), (None, 'dasdas', None, 'dasdh') )
assert( linkMatch( '[[category:dasdas]]' ), ('category', 'dasdas', None, None) )
assert( linkMatch( '[[category:dasdas#dasda]]' ), ('category', 'dasdas', 'dasda', None) )
assert( linkMatch( '[[category:dasdas#dasda|dasdh]]' ), ('category', 'dasdas', 'dasda', 'dasdh') )
assert( linkMatch( '[[category:dasdas|dasdh]]' ), ('category', 'dasdas', None, 'dasdh') )

redirectRE = re.compile('\#redirect[ ]*%s([ ]*\{\{([^\}]+)\}\})?' % linkREtxt)

def redirectMatch( text ):
    m = redirectRE.match( text )
    if m:
        namespace = m.group(2)
        linkPage = m.group(3)
        pageAnchor = m.group(4)
        alternateText = m.group(6)
        redirectReason = m.group(7)
        return (namespace, linkPage, pageAnchor, alternateText, redirectReason)
        
def processRedirect( pageTitle, pageText, processedConn ):
    rawText = rawText.replace( '\n', ' ' )
    m = redirectRE.match( rawText )
    if m:
        redirectFrom = title
        redirectTo = m.group(3)
        redirectReason = m.group(7)
        if redirectReason == None or redirectReason not in uninterestingReasons:
            redirectPages += 1
            if redirectTo in titleDict:
                toId = titleDict[redirectTo]
                processedConn.execute( 'INSERT INTO alternateNames VALUES( ?, ? )', [redirectFrom, toId] )

