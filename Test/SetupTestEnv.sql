SET SERVEROUTPUT ON

-----------------------------------------------------------------------------------------------------------------
-- Create tablespace    
-----------------------------------------------------------------------------------------------------------------
CREATE TABLESPACE ILM_HOT_TBS DATAFILE 'ILM_HOT_TBS.dat' SIZE 5M AUTOEXTEND ON;
CREATE TABLESPACE ILM_WARM_TBS DATAFILE 'ILM_WARM_TBS.dat' SIZE 5M AUTOEXTEND ON;
CREATE TABLESPACE ILM_COLD_TBS DATAFILE 'ILM_COLD_TBS.dat' SIZE 5M AUTOEXTEND ON;
CREATE TABLESPACE ILM_DORMANT_TBS DATAFILE 'ILM_DORMANT_TBS.dat' SIZE 5M AUTOEXTEND ON;

-----------------------------------------------------------------------------------------------------------------
-- Create ILM tables
-----------------------------------------------------------------------------------------------------------------
@@../ILM-Setup.sql;

-----------------------------------------------------------------------------------------------------------------
-- ILM tables population
-----------------------------------------------------------------------------------------------------------------
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-1, 'HOT_TABLESPACE_NAME', 'ILM_HOT_TBS', SYSTIMESTAMP);
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-2, 'WARM_TABLESPACE_NAME', 'ILM_WARM_TBS', SYSTIMESTAMP);
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-3, 'COLD_TABLESPACE_NAME', 'ILM_COLD_TBS', SYSTIMESTAMP);
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-4, 'DORMANT_TABLESPACE_NAME', 'ILM_DORMANT_TBS', SYSTIMESTAMP);
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-5, 'PARALLEL_DEGREE', '4', SYSTIMESTAMP);
INSERT INTO ILMCONFIG (ID, PARAM, VALUE, LASTMODIFIED) VALUES (-6, 'ONLINE_MOVE', 'FALSE', SYSTIMESTAMP);

INSERT INTO ILMMANAGEDTABLE(ID, TABLENAME, TEMPTABLENAME, COLDTABLENAME, HOTRETENTION, WARMRETENTION, COLDRETENTION, WARMCOMPRESSION, COLDCOMPRESSION, DORMANTCOMPRESSION, MOVESEQUENCE, HOTSTATUS, WARMSTATUS, COLDSTATUS, LASTMODIFIED)
VALUES (-1, 'TPAYMENTINTERCHANGE', 'TPAYMENTINTERCHANGETEMP', 'TPAYMENTINTERCHANGECOLD', 1, 1, 1, 'OLTP', 'OLTP', 'OLTP', 1, null, null, null, sysdate);

INSERT INTO ILMMANAGEDTABLE(ID, TABLENAME, TEMPTABLENAME, COLDTABLENAME, HOTRETENTION, WARMRETENTION, COLDRETENTION, WARMCOMPRESSION, COLDCOMPRESSION, DORMANTCOMPRESSION, MOVESEQUENCE, HOTSTATUS, WARMSTATUS, COLDSTATUS, LASTMODIFIED)
VALUES (-2, 'TPAYMENTTRANSACTION', 'TPAYMENTTRANSACTIONTEMP', 'TPAYMENTTRANSACTIONCOLD', 1, 1, 1, 'OLTP', 'OLTP', 'OLTP', 2, null, null, null, sysdate);
commit;

-----------------------------------------------------------------------------------------------------------------
-- Add test package
-----------------------------------------------------------------------------------------------------------------
@@Package-ILMTEST.sql;
@@PackageBody-ILMTEST.sql;