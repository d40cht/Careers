
# Users schema
 
# --- !Ups

SET COMPRESS_LOB LZF;
 
CREATE TABLE "Users" (
    "id"        bigint(20) NOT NULL AUTO_INCREMENT,
    "added"     timestamp DEFAULT NOW() NOT NULL,
    "email"     varchar(255) NOT NULL,
    "password"  varchar(255) NOT NULL,
    "fullName"  varchar(255) NOT NULL,
    "isAdmin"   boolean NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE "CVs" (
    "id"        bigint(20) NOT NULL AUTO_INCREMENT,
    "added"     timestamp DEFAULT NOW() NOT NULL,
    "userId"    bigint(20) NOT NULL,
    "pdf"       BLOB,
    "text"      CLOB NOT NULL,
    
    CONSTRAINT fk_CVs_userId FOREIGN KEY("userId") REFERENCES "Users"("id")
);

CREATE TABLE "CVMetaData" (
    "cvId"          bigint(20) NOT NULL,
    "added"     timestamp DEFAULT NOW() NOT NULL,
    "topicWeights"  CLOB NOT NULL,
    "wordCloud"     CLOB NOT NULL,
    
    CONSTRAINT fk_CVMetaData_cvId FOREIGN KEY("cvId") REFERENCES "CVs"("id")
);

 
# --- !Downs
 
DROP TABLE "Users";
