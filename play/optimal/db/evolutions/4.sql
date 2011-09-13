# --- !Ups

CREATE TABLE "Companies" (
    "id"                bigint(20) NOT NULL AUTO_INCREMENT,
    "name"              VARCHAR NOT NULL,
    "url"               VARCHAR NOT NULL,
    "nameMatch1"        VARCHAR NOT NULL,
    "nameMatch2"        VARCHAR NOT NULL,
    PRIMARY KEY ("id")
);

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
    "cvId"              bigint(20) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT fk_Position_userId FOREIGN KEY("userId") REFERENCES "Users"("id"),
    CONSTRAINT fk_Position_companyId FOREIGN KEY("companyId") REFERENCES "Companies"("id")
);

# --- !Downs

