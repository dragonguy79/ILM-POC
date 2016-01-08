-----------------------------------
-- Clean and install tablespace
-----------------------------------

-- remove all partitioned tables (thus the partitions, log segment and indexes)
BEGIN
  for tab in (select table_name from user_tables where tablespace_name is null)
  LOOP
    Execute immediate 'drop table ' || tab.table_name || ' cascade constraints PURGE';
  END LOOP;
END;
/


-- drop tablespace
DROP TABLESPACE ILM_COLD_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
DROP TABLESPACE ILM_WARM_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
DROP TABLESPACE ILM_HOT_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;

-- create tablespace    
CREATE TABLESPACE ILM_HOT_TBS DATAFILE 'ILM_HOT_TBS.dat' SIZE 5M AUTOEXTEND ON;
CREATE TABLESPACE ILM_WARM_TBS DATAFILE 'ILM_WARM_TBS.dat' SIZE 5M AUTOEXTEND ON;
CREATE TABLESPACE ILM_COLD_TBS DATAFILE 'ILM_COLD_TBS.dat' SIZE 5M AUTOEXTEND ON;

