import os

nameDict = {}
for l in open( 'test.names', 'r' ).readlines():
    l = l.strip()
    els = l.split( " " )
    id = int(els[0])
    weight = float(els[1])
    name = (' '.join(els[2:])).strip()
    nameDict[id] = (name, weight)

os.system( "/home/alexw/Desktop/Community_latest/convert -i test.graph -o test.bin -w test.weight" )
os.system( "/home/alexw/Desktop/Community_latest/community test.bin -w test.weight -l -1 > res.txt" )

mapping = {}
print len(nameDict)
for i in range(0, len(nameDict)):
    mapping[i] = [i]

newMapping = {}
lastId = -1
for l in open( "res.txt", "r").readlines():
    els = l.split(" ")
    
    fromId = int(els[0])
    toId = int(els[1])
    
    if fromId < lastId:
        if False:
            # Stop at level 1
            break
        mapping = newMapping
        newMapping = {}
    
    if toId not in newMapping:
        newMapping[toId] = []
        
    newMapping[toId] = newMapping[toId] + mapping[fromId]
    lastId = fromId


for (groupId, topicIds) in newMapping.items():
    highestWeight = ('', (False, -1e99))
    for topicId in topicIds:
        name, weight = nameDict[topicId]
        thisWeight = (name.startswith( "Category"), weight)
        if highestWeight[1] < thisWeight:
            highestWeight = (name, thisWeight)

    print 'Group id: ', groupId, highestWeight
    weighted = []
    for topicId in topicIds:
        name, weight = nameDict[topicId]
        weighted.append( (-weight, name) )
    weighted.sort()
    
    for weight, name in weighted:
        print '  ', name, -weight

    



