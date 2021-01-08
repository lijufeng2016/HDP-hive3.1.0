set hive.mapred.mode=nonstrict;
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_add_part1;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_add_part2;




-- check add partition without insert privilege
create table tpart_n0(i int, j int) partitioned by (k string);

alter table tpart_n0 add partition (k = '1') location '${system:test.tmp.dir}/a_uri_add_part1/';
alter table tpart_n0 add partition (k = '2') location '${system:test.tmp.dir}/a_uri_add_part2/';

select count(*) from tpart_n0;

analyze table tpart_n0 partition (k) compute statistics;
