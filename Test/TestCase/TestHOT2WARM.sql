----**********************************************************************************************************************************************
-- Test cases for HOT2WARM
----**********************************************************************************************************************************************
SET SERVEROUTPUT ON
BEGIN
-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------

  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that the moved partition are now in WARM tablespace
    -- assert: 1 partition in WARM
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');

  -- test that the moved subpartition are now in WARM tablespace
    -- assert: 16 subpartitions in tbs WARM
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');


  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that the moved partition are now in WARM tablespace
    -- assert: 1 partition in WARM
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');

  -- test that the moved subpartition are now in WARM tablespace
    -- assert: 16 subpartitions in tbs WARM
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS''');

-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
  
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that partitioned lob 
    -- assert: 3 partitioned lob (3 columns of LOB) in tbs HOT because lob are not moved
  ILM_TEST.TEST_SQL_COUNT(3, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_11'' AND (tablespace_name=''ILM_HOT_TBS'' OR tablespace_name is null) ');
  
  -- test that subpartitioned lob are in HOT tablespace, because lob are not moved
    -- assert 48 subpartitioned lob (3 columns of LOB x 16 subpartition) in tbs HOT
  ILM_TEST.TEST_SQL_COUNT(48, 'SELECT COUNT(*) from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where part.table_name=''TPAYMENTINTERCHANGE'' and subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME and part.partition_name=''P2015_11'' AND subpart.TABLESPACE_NAME=''ILM_HOT_TBS'' ');
  
  -- test lob indexes are in HOT tablespace, because lob are not moved
    -- assert that all lob index (3 column x 5 partition x 16 sub partition) are in ILM_HOT_TBS tablespace for TPAYMENTINTERCHANGE table
  ILM_TEST.TEST_SQL_COUNT(240, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name=l.INDEX_NAME where l.table_name= ''TPAYMENTINTERCHANGE'' and l.TABLESPACE_NAME=''ILM_HOT_TBS''  and ind.TABLESPACE_NAME=''ILM_HOT_TBS''');


  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that partitioned lob 
    -- assert: 3 partitioned lob (2 columns of LOB) in tbs HOT because lob are not moved
  ILM_TEST.TEST_SQL_COUNT(2, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_11'' AND (tablespace_name=''ILM_HOT_TBS'' OR tablespace_name is null) ');
  
  -- test that subpartitioned lob are in HOT tablespace, because lob are not moved
    -- assert 48 subpartitioned lob (2 columns of LOB x 16 subpartition) in tbs HOT
  ILM_TEST.TEST_SQL_COUNT(32, 'SELECT COUNT(*) from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where part.table_name=''TPAYMENTTRANSACTION'' and subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME and part.partition_name=''P2015_11'' AND subpart.TABLESPACE_NAME=''ILM_HOT_TBS'' ');
  
  -- test lob indexes are in HOT tablespace, because lob are not moved
    -- assert that all lob index (2 column x 4 partition x 16 sub partition) are in ILM_HOT_TBS tablespace for TPAYMENTINTERCHANGE table
  ILM_TEST.TEST_SQL_COUNT(128, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name=l.INDEX_NAME where l.table_name= ''TPAYMENTTRANSACTION'' and l.TABLESPACE_NAME=''ILM_HOT_TBS''  and ind.TABLESPACE_NAME=''ILM_HOT_TBS''');


-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
  
  -- test that all global index are usuable
    -- assert no USUSABLE status, and tablespace of global index is not moved
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_INDEXES ui where STATUS=''UNUSABLE''');
  
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that moved partitioned index are in WARM tablespace
    -- 1 record only in partition P2015_11 because only 1 local index was created
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name=ti.index_name where ti.TABLE_NAME=''TPAYMENTINTERCHANGE'' and pi.partition_name=''P2015_11'' and pi.tablespace_name=''ILM_WARM_TBS''');
  
  -- test that moved subpartitioned index are in WARM tablespace
    -- assert 16 subpartition index (1 partition x 16 subpartition) with USABLE status
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name=ti.index_name where ti.table_name=''TPAYMENTINTERCHANGE'' and spi.partition_name=''P2015_11'' and spi.status=''USABLE'' and spi.tablespace_name=''ILM_WARM_TBS''');
  
  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that no partitioned index in WARM tablespace
    -- because TPAYMENTTRANSACTION does not have any local index
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name=ti.index_name where ti.TABLE_NAME=''TPAYMENTTRANSACTION'' and pi.partition_name=''P2015_11''');
  
  -- test that no moved subpartitioned index are in WARM tablespace
    -- because TPAYMENTTRANSACTION does not have any local index
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name=ti.index_name where ti.table_name=''TPAYMENTTRANSACTION'' and spi.partition_name=''P2015_11''');
  
-----------------------------------------------------------------------------------------------------------------
-- Compression
-----------------------------------------------------------------------------------------------------------------
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that compression BASIC is applied to WARM for P2015_10 and P2015_11
    -- expect 16 subparittion per partition
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''BASIC''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGE'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''BASIC''');
 
  
  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that compression BASIC is applied to WARM for P2015_10 and P2015_11
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_WARM_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''BASIC''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTION'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_WARM_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''BASIC''');
 
  
END;
/