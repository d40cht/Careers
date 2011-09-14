# --- !Ups

DROP TABLE IF EXISTS "CVMatches";

CREATE TABLE "CVMatches" (
    "id"            bigint(20) NOT NULL AUTO_INCREMENT,
    "fromCVId"      bigint(20) NOT NULL,
    "toCVId"        bigint(20) NOT NULL,
    "distance"      DOUBLE NOT NULL,
    "matchVector"   BLOB NOT NULL,
    
    CONSTRAINT fk_CVMatches_fromId FOREIGN KEY("fromCVId") REFERENCES "MatchVector"("id"),
    CONSTRAINT fk_CVMatches_toId FOREIGN KEY("toCVId") REFERENCES "MatchVector"("id")
);

CREATE INDEX index_CVMatches_fromCVId ON "CVMatches"("fromCVId");

# --- !Downs

