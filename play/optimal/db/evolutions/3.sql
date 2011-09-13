# --- !Ups

CREATE TABLE "CVMatches" (
    "id"            bigint(20) NOT NULL AUTO_INCREMENT,
    "fromCVId"      bigint(20) NOT NULL,
    "toCVId"        bigint(20) NOT NULL,
    "distance"      double NOT NULL,
    "matchVector"   BLOB NOT NULL,
    PRIMARY KEY("id"),
    CONSTRAINT fk_CVMatches_fromId FOREIGN KEY("fromCVId") REFERENCES "CVs"("id"),
    CONSTRAINT fk_CVMatches_toId FOREIGN KEY("toCVId") REFERENCES "CVs"("id")
);

# --- !Downs
