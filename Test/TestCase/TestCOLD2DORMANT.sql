----**********************************************************************************************************************************************
-- Unit test for WARM2COLD
----**********************************************************************************************************************************************
SET SERVEROUTPUT ON
BEGIN
-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------
  -- test that the moved partition are now in DORMANT tablespace
    -- assert: 32 partition (2 tables x 16 partition) in DORMANT
  ILM_TEST.TEST_SQL_COUNT(32, 'SELECT COUNT(*)  FROM USER_TAB_PARTITIONS WHERE table_name in (''TPAYMENTINTERCHANGEP2015_10'',''TPAYMENTINTERCHANGEP2015_11'') AND TABLESPACE_NAME = ''ILM_DORMANT_TBS''');

  
  -- test that the moved subpartition are gone
    -- assert: no subpartition found for partition P2015_11
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*)  FROM USER_TAB_SUBPARTITIONS WHERE PARTITION_NAME in (''P2015_10'', ''P2015_11'')');    
  
-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
  -- test that partitioned lob are in DORMANT tablespace
    -- assert: 96 partitioned lob (2 tables x 3 lob column x 16 partition) in tbs ILM_WARM_TBS
  ILM_TEST.TEST_SQL_COUNT(96, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE table_name in (''TPAYMENTINTERCHANGEP2015_10'',''TPAYMENTINTERCHANGEP2015_11'')  AND tablespace_name=''ILM_DORMANT_TBS''');
  
  -- test that no lob exist anymore in partition P2015_11
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*)  FROM USER_LOB_PARTITIONS WHERE partition_name in (''P2015_10'', ''P2015_11'')');

  -- test that subpartitioned lob 
    -- assert no subpartitioned lob exist for moved partitions
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*)  from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where subpart.LOB_PARTITION_NAME = part.LOB_PARTITION_NAME and part.partition_name in (''P2015_10'', ''P2015_11'')');
  
  -- test lob indexes
    -- assert that 96 supartitioned lob index (2 tables x 3 lob column x 16 partition) are in DORMANT tablespace 
  ILM_TEST.TEST_SQL_COUNT(96, 'SELECT COUNT(*) from USER_IND_PARTITIONS ind inner join USER_LOBS l on ind.index_name = l.INDEX_NAME where l.table_name in (''TPAYMENTINTERCHANGEP2015_10'',''TPAYMENTINTERCHANGEP2015_11'') AND ind.TABLESPACE_NAME=''ILM_DORMANT_TBS'' AND l.TABLESPACE_NAME=''ILM_DORMANT_TBS''');    

-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
  -- test that all global index are usuable
    -- assert no USUSABLE status, and tablespace of global index is not moved
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_INDEXES ui where STATUS = ''UNUSABLE'' AND TABLE_NAME in (''TPAYMENTINTERCHANGE'', ''TPAYMENTINTERCHANGECOLD'')');
  
  -- test that partitioned index are in DORMANT (only LOB index here, cause DORMANT table has no normal index)
    -- assert 96 partitioned indexes  (2 tables x 3 lob column x 16 partition) are in DORMANT
  ILM_TEST.TEST_SQL_COUNT(96, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name = ti.index_name where ti.table_name in (''TPAYMENTINTERCHANGEP2015_10'',''TPAYMENTINTERCHANGEP2015_11'')');
    
  -- test that index in source partitions are gone
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_PARTITIONS pi inner join USER_INDEXES ti on pi.index_name = ti.index_name where pi.partition_name  in (''P2015_10'', ''P2015_11'')');
  
  -- test that no subpartitioned exist in DORMANT
  ILM_TEST.TEST_SQL_COUNT(0, 'SELECT COUNT(*) from USER_IND_SUBPARTITIONS spi inner join USER_INDEXES ti on spi.index_name = ti.index_name where ti.table_name in (''TPAYMENTINTERCHANGEP2015_10'',''TPAYMENTINTERCHANGEP2015_11'')');
  
END;
/
