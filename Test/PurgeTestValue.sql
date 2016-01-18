-----------------------------------------------------------------------------------------------------------------
-- Drop partitioned tables
-----------------------------------------------------------------------------------------------------------------
SET SERVEROUTPUT ON
-- remove all partitioned tables (thus the partitions, log segment and indexes)
BEGIN
  for tab in (select table_name from USER_PART_TABLES WHERE TABLE_NAME LIKE 'TP%')
  LOOP
    DBMS_OUTPUT.PUT_LINE('Removed table ' || tab.table_name);
    EXECUTE IMMEDIATE 'DROP TABLE ' || TAB.TABLE_NAME || ' CASCADE CONSTRAINTS PURGE';
  END LOOP;
END;
/


-----------------------------------------------------------------------------------------------------------------
-- Delete test records from ILM tables
-----------------------------------------------------------------------------------------------------------------
DELETE FROM ILMJOB WHERE FROMTABLESPACE LIKE 'ILM_%_TBS';

DELETE FROM ILMCONFIG WHERE ID < 0;
DELETE FROM ILMMANAGEDTABLE WHERE ID < 0;

purge recyclebin;
COMMIT;
