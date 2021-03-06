SET SERVEROUTPUT ON

-- remove all test value
@@PurgeTestValue.sql;

-- drop test package
drop package body "ILM_TEST";
drop package "ILM_TEST";

-- remove ILM related tables
@@../ILM-Purge.sql;

-- empty recyclebin
PURGE RECYCLEBIN;  -- must purge recyclebin otherwise tablespace cannot be dropped if there are objects in it

-- drop tablespace
DROP TABLESPACE ILM_DORMANT_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
DROP TABLESPACE ILM_COLD_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
DROP TABLESPACE ILM_WARM_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
DROP TABLESPACE ILM_HOT_TBS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;
