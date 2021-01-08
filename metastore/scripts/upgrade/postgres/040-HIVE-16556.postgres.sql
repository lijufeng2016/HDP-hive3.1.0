CREATE TABLE "METASTORE_DB_PROPERTIES"
(
  "PROPERTY_KEY" VARCHAR(255) NOT NULL,
  "PROPERTY_VALUE" VARCHAR(1000) NOT NULL,
  "DESCRIPTION" VARCHAR(1000)
);

ALTER TABLE ONLY "METASTORE_DB_PROPERTIES"
  ADD CONSTRAINT "PROPERTY_KEY_PK" PRIMARY KEY ("PROPERTY_KEY");
