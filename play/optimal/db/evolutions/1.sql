# --- !Ups

CREATE SEQUENCE "seq_Users_id";
CREATE TABLE "Users" (
    "id"        BIGINT DEFAULT nextval('"seq_Users_id"') PRIMARY KEY,
    "added"     timestamp DEFAULT NOW() NOT NULL,
    "email"     varchar(255) NOT NULL,
    "password"  varchar(255) NOT NULL,
    "fullName"  varchar(255) NOT NULL,
    "isAdmin"   boolean NOT NULL
);

CREATE SEQUENCE "seq_CVs_id";
CREATE TABLE "CVs" (
    "id"                BIGINT DEFAULT nextval('"seq_CVs_id"') PRIMARY KEY,
    "added"             timestamp DEFAULT NOW() NOT NULL,
    "description"       varchar(255) NOT NULL,  
    "userId"            BIGINT NOT NULL REFERENCES "Users"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "pdf"               BYTEA,
    "text"              BYTEA NOT NULL,
    "documentDigest"    BYTEA
);


CREATE SEQUENCE "seq_Companies_id";
CREATE TABLE "Companies" (
    "id"                BIGINT DEFAULT nextval('"seq_Companies_id"') PRIMARY KEY,
    "name"              VARCHAR NOT NULL,
    "description"       VARCHAR NOT NULL,
    "url"               VARCHAR NOT NULL,
    "nameMatch1"        VARCHAR NOT NULL,
    "nameMatch2"        VARCHAR NOT NULL
);

CREATE SEQUENCE "seq_MatchVector_id";
CREATE TABLE "MatchVector" (
    "id"                BIGINT DEFAULT nextval('"seq_MatchVector_id"') PRIMARY KEY,
    "cvId"              BIGINT REFERENCES "CVs"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "topicVector"       BYTEA NOT NULL
);


CREATE SEQUENCE "seq_Position_id";
CREATE TABLE "Position" (
    "id"                BIGINT DEFAULT nextval('"seq_Position_id"') PRIMARY KEY,
    "userId"            BIGINT REFERENCES "Users"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "companyId"         BIGINT REFERENCES "Companies"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "department"        VARCHAR NOT NULL,
    "jobTitle"          VARCHAR NOT NULL,
    "yearsExperience"   INTEGER NOT NULL,
    "startYear"         INTEGER NOT NULL,
    "endYear"           INTEGER NOT NULL,
    "address"           VARCHAR NOT NULL,
    "longitude"         REAL NOT NULL,
    "latitude"          REAL NOT NULL,
    "matchVectorId"     BIGINT REFERENCES "MatchVector"("id") ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE SEQUENCE "seq_Matches_id";
CREATE TABLE "Matches" (
    "id"            BIGINT DEFAULT nextval('"seq_Matches_id"') PRIMARY KEY,
    "fromMatchId"   BIGINT REFERENCES "MatchVector"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "toMatchId"     BIGINT REFERENCES "MatchVector"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "similarity"    REAL NOT NULL,
    "matchVector"   BYTEA NOT NULL,
    "added"         timestamp DEFAULT NOW() NOT NULL
);
CREATE INDEX index_Matches_fromCVId ON "Matches"("fromMatchId");

CREATE SEQUENCE "seq_Searches_id";
CREATE TABLE "Searches" (
    "id"            BIGINT DEFAULT nextval('"seq_Searches_id"') PRIMARY KEY,
    "userId"        BIGINT REFERENCES "Users"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "description"   VARCHAR NOT NULL,
    "address"       VARCHAR NOT NULL,
    "longitude"     REAL NOT NULL,
    "latitude"      REAL NOT NULL,
    "radius"        REAL NOT NULL,
    "matchVectorId" BIGINT REFERENCES "MatchVector"("id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE SEQUENCE "seq_Logs_id";
CREATE TABLE "Logs" (
    "id"            BIGINT DEFAULT nextval('"seq_Logs_id"') PRIMARY KEY,
    "time"          timestamp default current_timestamp NOT NULL,
    "userId"        BIGINT REFERENCES "Users"("id") ON UPDATE CASCADE ON DELETE CASCADE,
    "event"         VARCHAR
);

# --- Rev:1, Downs
