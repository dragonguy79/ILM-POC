----**********************************************************************************************************************************************
-- Unit test for WARM2COLD
----**********************************************************************************************************************************************
SET SERVEROUTPUT ON
BEGIN
-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------

  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that the moved partition are now in COLD tablespace
    -- assert: 1 partition in ILM_COLD_TBS
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  
  -- test that the moved subpartition are now in COLD tablespace
    -- assert: 16 subpartitions in tbs ILM_COLD_TBS
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  
  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that the moved partition are now in COLD tablespace
    -- assert: 1 partition in ILM_COLD_TBS
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  
  -- test that the moved subpartition are now in COLD tablespace
    -- assert: 16 subpartitions in tbs ILM_COLD_TBS
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS''');
  
-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that partitioned lob are in COLD tablespace
    -- assert: 3 partitioned lob (3 columns of LOB) in tbs ILM_WARM_TBS
  ILM_TEST.TEST_SQL_COUNT(3, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_11'' AND tablespace_name=''ILM_COLD_TBS''');
  
  -- test that subpartitioned lob are in COLD tablespace
    -- assert 48 subpartitioned lob (3 columns of LOB x 16 subpartition) in tbs HOT
  ILM_TEST.TEST_SQL_COUNT(48, 'SELECT COUNT(*) from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where part.TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME and part.partition_name=''P2015_11'' AND subpart.TABLESPACE_NAME=''ILM_COLD_TBS'' ');

  -- test lob indexes
    -- assert that all lob index (3 column x 3 partition(2 existing + 1 moved in) x 16 sub partition) are in COLD tablespace for TPAYMENTINTERCHANGECOLD table
  ILM_TEST.TEST_SQL_COUNT(144, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name=l.INDEX_NAME where l.table_name= ''TPAYMENTINTERCHANGECOLD'' and l.TABLESPACE_NAME=''ILM_COLD_TBS'' and ind.TABLESPACE_NAME=''ILM_COLD_TBS''');

  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that partitioned lob are in COLD tablespace
    -- assert: 3 partitioned lob (2 columns of LOB) in tbs ILM_WARM_TBS
  ILM_TEST.TEST_SQL_COUNT(2, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_11'' AND tablespace_name=''ILM_COLD_TBS''');
  
  -- test that subpartitioned lob are in COLD tablespace
    -- assert 48 subpartitioned lob (2 columns of LOB x 16 subpartition) in tbs HOT
  ILM_TEST.TEST_SQL_COUNT(32, 'SELECT COUNT(*) from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where part.TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME and part.partition_name=''P2015_11'' AND subpart.TABLESPACE_NAME=''ILM_COLD_TBS'' ');

  -- test lob indexes
    -- assert that all lob index (2 column x 3 partition(2 move in + 1 existing P9999_12) x 16 sub partition) are in ILM_HOT_TBS tablespace for TPAYMENTINTERCHANGECOLD table
  ILM_TEST.TEST_SQL_COUNT(96, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name=l.INDEX_NAME where l.table_name= ''TPAYMENTTRANSACTIONCOLD'' and l.TABLESPACE_NAME=''ILM_COLD_TBS''  and ind.TABLESPACE_NAME=''ILM_COLD_TBS''');

-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that all global index are usuable
    -- assert no USUSABLE status, and tablespace of global index is not moved
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_INDEXES ui where STATUS=''UNUSABLE'' AND TABLE_NAME in (''TPAYMENTINTERCHANGE'', ''TPAYMENTINTERCHANGECOLD'')');
  
  
  -- test that moved partitioned index are in COLD tablespace
   -- 1 record only in partition P2015_11 because only 1 local index was created
  ILM_TEST.TEST_SQL_COUNT(1, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name=ti.index_name  where TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' and pi.partition_name=''P2015_11'' and pi.tablespace_name=''ILM_COLD_TBS''');
  
  -- test that moved subpartitioned index are in COLD tablespace
    -- assert 16 subpartition index (1 partition x 16 subpartition) with USABLE status
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name=ti.index_name where ti.table_name=''TPAYMENTINTERCHANGECOLD'' and spi.partition_name=''P2015_11'' and spi.status=''USABLE'' and spi.tablespace_name=''ILM_COLD_TBS''');
  
  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that all global index are usuable
    -- assert no USUSABLE status, and tablespace of global index is not moved
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_INDEXES ui where STATUS=''UNUSABLE'' AND TABLE_NAME in (''TPAYMENTTRANSACTION'', ''TPAYMENTTRANSACTIONCOLD'')');
  
  
  -- test that no partitioned index are moved to COLD tablespace
   -- because TPAYMENTTRANSACTION does not have any local index
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name=ti.index_name where TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' and pi.partition_name=''P2015_11''');
  
  -- test that no subpartitioned index are moved to COLD tablespace
    -- because TPAYMENTTRANSACTION does not have any local index
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name=ti.index_name where ti.table_name=''TPAYMENTTRANSACTIONCOLD'' and spi.partition_name=''P2015_11'' ');
  
-----------------------------------------------------------------------------------------------------------------
-- Compression
-----------------------------------------------------------------------------------------------------------------
  -- ******************** TPAYMENTINTERCHANGE ********************-- 
  -- test that compression OLTP is applied to WARM for P2015_10 and P2015_11
    -- expect 16 subparittion per partition
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''ADVANCED''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTINTERCHANGECOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''ADVANCED''');
 
  
  -- ******************** TPAYMENTTRANSACTION ********************-- 
  -- test that compression OLTP is applied to WARM for P2015_10 and P2015_11
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_10'' AND TABLESPACE_NAME=''ILM_COLD_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''ADVANCED''');
  ILM_TEST.TEST_SQL_COUNT(16, 'SELECT COUNT(*) FROM USER_TAB_SUBPARTITIONS WHERE TABLE_NAME=''TPAYMENTTRANSACTIONCOLD'' AND PARTITION_NAME=''P2015_11'' AND TABLESPACE_NAME=''ILM_COLD_TBS'' AND COMPRESSION=''ENABLED'' AND COMPRESS_FOR=''ADVANCED''');
 
 
END;
/