-----------------------------------------------
-- ILM table
-----------------------------------------------
select ijob.*, endtime - starttime as duration from ILMJOB ijob order by ID DESC;
select itask.*, endtime - starttime from ILMTASK itask order by ID DESC;
select * from ILMLOG order by ID DESC;      
select * from ILMMANAGEDTABLE;
select * from ILMCONFIG;

select * from ILMJOB order by ID DESC;
select * from ILMLOG where log like '%JOBID='||(select max(ID) FROM ILMJOB)||'%' order by ID DESC;

select * from ILMTASK order by ID DESC;
select * from ILMTASK where jobID = 46;
  
-----------------------------------------------
-- Drop objects
-----------------------------------------------

TRUNCATE TABLE ILMJOB;
TRUNCATE TABLE ILMTASK;
TRUNCATE TABLE ILMLOG;
TRUNCATE TABLE ILMMANAGEDTABLE;
TRUNCATE TABLE ILMCONFIG;

DROP TABLE PAYMENTINTERCHANGE PURGE;

DROP TABLE ILMJOB CASCADE CONSTRAINTS PURGE;
DROP TABLE ILMTASK CASCADE CONSTRAINTS PURGE;
DROP TABLE ILMLOG CASCADE CONSTRAINTS PURGE;
DROP TABLE ILMMANAGEDTABLE CASCADE CONSTRAINTS PURGE;
DROP TABLE ILMCONFIG CASCADE CONSTRAINTS PURGE;


-----------------------------------------------
-- Partition
-----------------------------------------------
-- metadata of data partittion  -----------------------------------------------------------------------------------------
select * from USER_TAB_PARTITIONS;    -- list all subpartitions
select * from USER_TAB_SUBPARTITIONS;    -- list all subpartitions
select * from USER_SUBPART_COL_STATISTICS;   -- list all subpartitions and their columns
select * from USER_PART_TABLES;      -- list all partitioned tables

-- metadata of index partittion  -----------------------------------------------------------------------------------------
select status, ui.* from USER_INDEXES ui;
select * from USER_PART_INDEXES;  --  list all partitioned indexes
select status, ui.* from USER_IND_PARTITIONS ui;    -- llist index partitions (local indexes)
select status, ui.* from USER_IND_SUBPARTITIONS ui;    -- list index sub-partitions (sub-partition local indexes)

-- metadata of lob partittion  -----------------------------------------------------------------------------------------
select * from USER_LOBS;
select * from USER_PART_LOBS; -- 1 row per table
select * from USER_LOB_PARTITIONS;  --  list all partition lob, 1 row per partition
select * from USER_LOB_SUBPARTITIONS; -- list all subpartitions lob


-----------------------------------------------
-- Payment Interchange
-----------------------------------------------
truncate table PAYMENTINTERCHANGE;

select * from PAYMENTINTERCHANGE order by PAYMENTINTERCHANGEKEY;
select * from PAYMENTINTERCHANGE partition(P2015_11);
select * from PAYMENTINTERCHANGE partition(P2015_12);
select * from PAYMENTINTERCHANGE partition(P2016_01);
select * from PAYMENTINTERCHANGE SUBPARTITION(SYS_SUBP12007);


select * from PAYMENTINTERCHANGETEMP;
select * from PAYMENTINTERCHANGECOLD partition(P2015_11);

-- list count from partition
SET SERVEROUTPUT ON;
declare 
  icount number;
BEGIN
  FOR pRow in (SELECT PARTITION_NAME FROM USER_TAB_PARTITIONS WHERE TABLE_NAME='PAYMENTINTERCHANGE')
  LOOP
    execute immediate 'select count(*)  from PAYMENTINTERCHANGE partition(' || pRow.PARTITION_NAME || ')' into icount;
    DBMS_OUTPUT.PUT_LINE(pRow.PARTITION_NAME || ' - ' || icount);
  END LOOP;
END;
/


