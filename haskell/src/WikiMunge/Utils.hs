module WikiMunge.Utils where

import Data.Binary

data LazySerializingList a = LazySerializingList [a] deriving (Eq, Show)

chunkList list chunkSize =
    let soFar = take chunkSize list in
    let size = length soFar in
    if (size == 0) then
        [(0, [])]
    else
        (size, soFar) : chunkList (drop size list) chunkSize

incrementalPut list chunkSize =
    mapM_ (\x -> put (fst x) >> put (snd x)) $ chunkList list chunkSize

{-
-- The type signature below means that for a Binary serializable object 'a' ('Binary a =>'),
-- this instance will serialize a list of [a] (Binary [a])

instance Binary a => Binary [a] where
    put l  = put (length l) >> mapM_ put l
    get    = do n <- get :: Get Int
                replicateM n get
-}

getLazySerializingList :: Binary a => Get [a]
getLazySerializingList = do
    length <- get :: Get Int
    headChunk <- get
    tailChunk <- if (length==0) then do
        return []
        else do
            tailData <- getLazySerializingList
            return tailData
    return $ headChunk ++ tailChunk

instance (Binary a) => Binary (LazySerializingList a) where
    put (LazySerializingList l) = incrementalPut l 255
    get = do
        listData <- getLazySerializingList
        return $ LazySerializingList listData
   
