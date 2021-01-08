SELECT 'Upgrading MetaStore schema from 1.2.1000 to 2.0.0' AS ' ';
SOURCE 021-HIVE-7018.mysql.sql;
SOURCE 022-HIVE-11970.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.0.0', VERSION_COMMENT='Hive release version 2.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.1000 to 2.0.0' AS ' ';

