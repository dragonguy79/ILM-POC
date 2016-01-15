----**********************************************************************************************************************************************
-- Unit test for WARM2COLD
----**********************************************************************************************************************************************

-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------
-- test that the moved partition are now in COLD tablespace
  -- assert: 1 partition in ILM_WARM_TBS
select TABLE_NAME, partition_name, tablespace_name  from USER_TAB_PARTITIONS where partition_name = 'P2015_11';


-- test that the moved subpartition are now in COLD tablespace
  -- assert: 16 subpartitions in tbs ILM_WARM_TBS
select TABLE_NAME, partition_name, tablespace_name  from USER_TAB_SUBPARTITIONS where partition_name = 'P2015_11';

-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
-- test that partitioned lob are in COLD tablespace
  -- assert: 3 partitioned lob (3 columns of LOB) in tbs ILM_WARM_TBS
select  TABLE_NAME, column_name, partition_name, LOB_PARTITION_NAME, tablespace_name from USER_LOB_PARTITIONS where partition_name = 'P2015_11';

-- test that subpartitioned lob are in HOT tablespace, because lob are not moved
  -- assert 48 subpartitioned lob (3 columns of LOB x 16 subpartition) in tbs ILM_HOT_TBS
select subpart.TABLE_NAME, subpart.column_name, subpart.subpartition_name,subpart.LOB_PARTITION_NAME, subpart.tablespace_name  
from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where subpart.LOB_PARTITION_NAME = part.LOB_PARTITION_NAME
and part.partition_name = 'P2015_11';

-- test lob indexes are in HOT tablespace, because lob are not moved
  -- assert that all lob index are in ILM_HOT_TBS tablespace for PAYMENTINTERCHANGECOLD table
select ind.index_name, l.table_name, l.column_name, ind.TABLESPACE_NAME as index_tbs, l.TABLESPACE_NAME as lob_tbs 
from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name = l.INDEX_NAME
where l.table_name= 'PAYMENTINTERCHANGE' ;

  -- assert that all supartitioned lob index are in HOT tablespace for PAYMENTINTERCHANGE table
select ind.index_name, l.table_name, l.column_name, ind.TABLESPACE_NAME as index_tbs, l.TABLESPACE_NAME as lob_tbs 
from USER_IND_SUBPARTITIONS ind inner join USER_LOBS l on ind.index_name = l.INDEX_NAME
where l.table_name= 'PAYMENTINTERCHANGE' ;


-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
-- test that all global index are usuable
  -- assert no USUSABLE status, and tablespace of global index is not moved
select status, tablespace_name, ui.* from USER_INDEXES ui where STATUS = 'UNUSABLE';


-- test that moved partitioned index are in COLD tablespace
  -- assert status remain N/A, and tablespace is in COLD
select pi.index_name, pi.status, pi.partition_name, pi.tablespace_name, ti.table_name
from USER_IND_PARTITIONS pi 
inner join USER_INDEXES ti on pi.index_name = ti.index_name
where pi.partition_name = 'P2015_11' ;

-- test that moved subpartitioned index are in COLD tablespace
  -- assert status=USABLE, and tablespace is in COLD
select spi.index_name, spi.status, spi.partition_name, spi.tablespace_name, ti.table_name
from USER_IND_SUBPARTITIONS spi 
inner join USER_INDEXES ti on spi.index_name = ti.index_name
where spi.partition_name = 'P2015_11' ;


