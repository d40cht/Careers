

# --- !Ups

ALTER TABLE "CVs" ADD COLUMN "documentDigest" BLOB;
DROP TABLE "CVMetaData";

# --- !Downs
 
