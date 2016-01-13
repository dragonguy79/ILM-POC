----**********************************************************************************************************************************************
-- Unit test for WARM2COLD
----**********************************************************************************************************************************************
SET SERVEROUTPUT ON
BEGIN
-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------

  -- test that the moved partition are now in COLD tablespace
    -- assert: 1 partition in WARM
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE PARTITION_NAME = ''P2015_11'' AND TABLESPACE_NAME = ''ILM_WARM_TBS''');

  -- test that the moved subpartition are now in COLD tablespace
    -- assert: 16 subpartitions in tbs WARM
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE PARTITION_NAME = ''P2015_11'' AND TABLESPACE_NAME = ''ILM_WARM_TBS''');


-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
  -- test that partitioned lob 
    -- assert: 3 partitioned lob (3 columns of LOB) in tbs HOT because lob are not moved
  ILM_TEST.TEST_SQL_COUNT(3, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE PARTITION_NAME = ''P2015_11'' AND (tablespace_name=''ILM_HOT_TBS'' OR tablespace_name is null) ');

  
  -- test that subpartitioned lob are in HOT tablespace, because lob are not moved
    -- assert 48 subpartitioned lob (3 columns of LOB x 16 subpartition) in tbs HOT
  ILM_TEST.TEST_SQL_COUNT(48, 'SELECT COUNT(*) from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME and part.partition_name=''P2015_11'' AND subpart.TABLESPACE_NAME=''ILM_HOT_TBS'' ');

  
  -- test lob indexes are in HOT tablespace, because lob are not moved
    -- assert that all lob index (3 column x 4 partition x 16 sub partition) are in ILM_HOT_TBS tablespace for PAYMENTINTERCHANGECOLD table
  ILM_TEST.TEST_SQL_COUNT(192, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name = l.INDEX_NAME where l.table_name= ''PAYMENTINTERCHANGE'' and l.TABLESPACE_NAME=''ILM_HOT_TBS''  and ind.TABLESPACE_NAME=''ILM_HOT_TBS''');


-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
  -- test that all global index are usuable
    -- assert no USUSABLE status, and tablespace of global index is not moved
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_INDEXES ui where STATUS = ''UNUSABLE''');
  
  
  -- test that moved partitioned index are in WARM tablespace
    -- 1 record only in partition P2015_11 because only 1 local index was created
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name = ti.index_name  where pi.partition_name = ''P2015_11'' and pi.tablespace_name=''ILM_WARM_TBS''');
  
  
  -- test that moved subpartitioned index are in WARM tablespace
    -- assert 16 subpartition index (1 partition x 16 subpartition) with USABLE status
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name = ti.index_name where ti.table_name=''PAYMENTINTERCHANGE'' and spi.partition_name = ''P2015_11'' and spi.status=''USABLE'' and spi.tablespace_name=''ILM_WARM_TBS''');
  

END;
/