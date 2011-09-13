

# --- !Ups

CREATE TABLE "CVMatches" (
    "id"            bigint(20) NOT NULL AUTO_INCREMENT,
    "fromCVId"      bigint(20) NOT NULL,
    "toCVId"        bigint(20) NOT NULL,
    "distance"      DOUBLE NOT NULL,
    "matchVector"   BLOB NOT NULL,
    
    CONSTRAINT fk_CVMatches_fromCVId FOREIGN KEY("fromCVId") REFERENCES "CVs"("id"),
    CONSTRAINT fk_CVMatches_toCVId FOREIGN KEY("toCVId") REFERENCES "CVs"("id")
);

CREATE INDEX index_CVs_Id ON "CVs"("id");
CREATE INDEX index_CVMatches_fromCVId ON "CVMatches"("fromCVId");

# --- !Downs
 
