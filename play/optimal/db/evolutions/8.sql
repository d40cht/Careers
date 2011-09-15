# --- !Ups


ALTER TABLE "Companies" ADD "description" VARCHAR DEFAULT '' NOT NULL BEFORE "nameMatch1";

# --- !Downs

