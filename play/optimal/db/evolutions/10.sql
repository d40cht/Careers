# --- !Ups

CREATE TABLE "Logs" (
    "id"            bigint(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,
    "time"          timestamp default current_timestamp() NOT NULL,
    "userId"        bigint(20) NOT NULL,
    "event"         VARCHAR,
    
    CONSTRAINT fk_Logs_userId FOREIGN KEY("userId") REFERENCES "Users"("id")
);

