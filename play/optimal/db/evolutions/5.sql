# --- !Ups

DROP TABLE IF EXISTS "MatchVector";

CREATE TABLE "MatchVector" (
    "id"                bigint(20) NOT NULL AUTO_INCREMENT,
    "cvId"              bigint(20) NOT NULL,
    "topicVector"       BLOB NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT fk_MatchVector_cvId FOREIGN KEY("cvId") REFERENCES "CVs"("id")
);


DROP TABLE IF EXISTS "Position";

CREATE TABLE "Position" (
    "id"                bigint(20) NOT NULL AUTO_INCREMENT,
    "userId"            bigint(20) NOT NULL,
    "companyId"         bigint(20) NOT NULL,
    "department"        VARCHAR,
    "jobTitle"          VARCHAR NOT NULL,
    "yearsExperience"   INTEGER NOT NULL,
    "startYear"         INTEGER NOT NULL,
    "endYear"           INTEGER NOT NULL,
    "longitude"         DOUBLE NOT NULL,
    "latitude"          DOUBLE NOT NULL,
    "matchVectorId"     bigint(20) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT fk_Position_userId FOREIGN KEY("userId") REFERENCES "Users"("id"),
    CONSTRAINT fk_Position_companyId FOREIGN KEY("companyId") REFERENCES "Companies"("id"),
    CONSTRAINT fk_Position_matchVectorId FOREIGN KEY("matchVectorId") REFERENCES "MatchVector"("id")
);

# --- !Downs

