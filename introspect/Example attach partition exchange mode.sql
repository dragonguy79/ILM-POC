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

-- create temp table
CREATE TABLE PAYMENTINTERCHANGETEMPWARM
(	 
     PAYMENTINTERCHANGEKEY   NUMBER(38,0) NOT NULL,
     INCOMING                CHAR(1) NOT NULL,
     DISPLAYINTERCHANGEID    VARCHAR2(128) NOT NULL,
     FILENAME                VARCHAR2(256),
     FILEFORMAT              VARCHAR2(128),
     TRANSPORTTIME           DATE,
     STATUS                  VARCHAR2(30),
     TRANSPORTDATA           BLOB DEFAULT EMPTY_BLOB(),
     DETAILS                 XMLTYPE,
     PROCESSINGDETAILS       XMLTYPE,
     BANKGROUPID             VARCHAR2(30) NOT NULL,
     CUSTOMERPAYMENTTYPEKEY  NUMBER(38,0),
     CLEARINGCONDITIONKEY    NUMBER(38,0) NOT NULL,
     BANKPAYMENTTYPEKEY      NUMBER(38,0),
     BANKINGENTITYKEY        NUMBER(38,0),
     ILMKEY                 TIMESTAMP default TO_TIMESTAMP('10-DEC-2015:10:10:10','DD-MON-YYYY:HH24:MI:SS'),
     VERSION                NUMBER(38,0) NOT NULL ENABLE,
     WHENMODIFIED            DATE NOT NULL ENABLE,
     CONSTRAINT "PK_PAYMENTINTERCHANGETEMPWARM" PRIMARY KEY ("PAYMENTINTERCHANGEKEY"))
XMLTYPE COLUMN DETAILS STORE AS SECUREFILE CLOB (TABLESPACE ILM_WARM_TBS ENABLE STORAGE IN ROW COMPRESS LOW CACHE)
XMLTYPE COLUMN PROCESSINGDETAILS STORE AS SECUREFILE CLOB (TABLESPACE ILM_WARM_TBS ENABLE STORAGE IN ROW COMPRESS LOW CACHE)
TABLESPACE ILM_WARM_TBS
PARTITION BY HASH(PAYMENTINTERCHANGEKEY) PARTITIONS 16;


-- create temp table with partition
--DROP table PAYMENTINTERCHANGETEMPWARM purge;
--create table PAYMENTINTERCHANGETEMPWARM
--  PARTITION BY HASH(PAYMENTINTERCHANGEKEY) PARTITIONS 16
--  tablespace ILM_WARM_TBS as
--  select * from PAYMENTINTERCHANGE
--  where 1=2
--  ;

-- split high bound partition in target table
ALTER TABLE paymentinterchangecold SPLIT PARTITION pc9999_12 AT (to_date('2015-12-01', 'yyyy-MM-dd')) INTO (PARTITION pc2015_11, PARTITION pc9999_12);
  
-- exchange partition with temp table
ALTER TABLE PAYMENTINTERCHANGE 
EXCHANGE PARTITION P2015_11 WITH TABLE PAYMENTINTERCHANGETEMPWARM
WITHOUT VALIDATION;

BEGIN
  FOR pRow in (select PARTITION_NAME from USER_TAB_PARTITIONS WHERE TABLE_NAME='PAYMENTINTERCHANGETEMPWARM')
  LOOP
    -- DBMS_OUTPUT.PUT_LINE(pRow.PARTITION_NAME);
    EXECUTE IMMEDIATE 'ALTER TABLE PAYMENTINTERCHANGETEMPWARM MOVE PARTITION ' || pRow.PARTITION_NAME||' TABLESPACE ILM_COLD_TBS PARALLEL (DEGREE 2)';
  END LOOP;
END;
/

-- move records to target tablespace
ALTER TABLE PAYMENTINTERCHANGETEMPWARM MOVE TABLESPACE ILM_COLD_TBS;

-- exchange partition with temp table
ALTER TABLE PAYMENTINTERCHANGECOLD 
EXCHANGE PARTITION PC2015_11 WITH TABLE PAYMENTINTERCHANGETEMPWARM
WITHOUT VALIDATION;

-- drop source partition
ALTER TABLE PAYMENTINTERCHANGE DROP PARTITION P2015_11;

select * from PAYMENTINTERCHANGE partition(P2015_11);
select * from PAYMENTINTERCHANGETEMPWARM;
select * from PAYMENTINTERCHANGECOLD partition(PC2015_11);



