# --- !Ups

DROP TABLE IF EXISTS "CVMatches";

CREATE TABLE "Matches" (
    "id"            bigint(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,
    "fromMatchId"   bigint(20) NOT NULL,
    "toMatchId"     bigint(20) NOT NULL,
    "similarity"    DOUBLE NOT NULL,
    "matchVector"   BLOB NOT NULL,
    
    CONSTRAINT fk_Matches_fromId FOREIGN KEY("fromMatchId") REFERENCES "MatchVector"("id"),
    CONSTRAINT fk_Matches_toId FOREIGN KEY("toMatchId") REFERENCES "MatchVector"("id")
);

CREATE INDEX index_Matches_fromCVId ON "Matches"("fromMatchId");


CREATE TABLE "Searches" (
    "id"            bigint(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,
    "userId"        bigint(20) NOT NULL,
    "description"   VARCHAR NOT NULL,
    "longitude"     DOUBLE NOT NULL,
    "latitude"      DOUBLE NOT NULL,
    "radius"        DOUBLE NOT NULL,
    "matchVectorId" bigint(20) NOT NULL,
    
    CONSTRAINT fk_Searches_userId FOREIGN KEY("userId") REFERENCES "Users"("id"),
    CONSTRAINT fk_Searches_matchVectorId FOREIGN KEY("matchVectorId") REFERENCES "MatchVector"("id")
);

# --- !Downs

