-- Make a second PDB with the single-byte charset WE8ISO8859P1 (the CDB is AL32UTF8).
whenever sqlerror exit 1
alter session set container=CDB$ROOT;
create pluggable database GH759WE8 admin user pdbadmin identified by sling_gh759
  file_name_convert=('/opt/oracle/oradata/FREE/pdbseed/','/opt/oracle/oradata/FREE/GH759WE8/');
alter pluggable database GH759WE8 open restricted;
alter session set container=GH759WE8;
alter database character set internal_use WE8ISO8859P1;
alter session set container=CDB$ROOT;
alter pluggable database GH759WE8 close immediate;
alter pluggable database GH759WE8 open;
alter session set container=GH759WE8;
create user sling identified by sling_gh759 quota unlimited on system;
grant connect, resource, create table to sling;
exit
