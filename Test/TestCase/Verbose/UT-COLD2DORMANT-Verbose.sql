----**********************************************************************************************************************************************
-- Unit test for WARM2COLD
----**********************************************************************************************************************************************

-----------------------------------------------------------------------------------------------------------------
-- Data
-----------------------------------------------------------------------------------------------------------------
-- test that the moved partition are now in DORMANT tablespace
  -- assert: 1 partition in ILM_COLD_TBS
select TABLE_NAME, partition_name, tablespace_name from USER_TAB_PARTITIONS where table_name in ('TPAYMENTINTERCHANGEP2015_10','TPAYMENTINTERCHANGEP2015_11'); 


-- test that the moved subpartition are gone
  -- assert: no subpartition found for partition P2015_11
select TABLE_NAME, partition_name, tablespace_name from USER_TAB_SUBPARTITIONS where table_name='TPAYMENTINTERCHANGE' and partition_name in ('P2015_10', 'P2015_11');

-----------------------------------------------------------------------------------------------------------------
-- Lob
-----------------------------------------------------------------------------------------------------------------
-- test that partitioned lob are in DORMANT tablespace
  -- assert: 3 partitioned lob (3 columns of LOB) in tbs ILM_DORMANT_TBS
select  TABLE_NAME, column_name, partition_name, LOB_PARTITION_NAME, tablespace_name from USER_LOB_PARTITIONS where table_name in ('TPAYMENTINTERCHANGEP2015_10','TPAYMENTINTERCHANGEP2015_11'); 

-- test that no lob exist anymore in partition P2015_11
select  TABLE_NAME, column_name, partition_name, LOB_PARTITION_NAME, tablespace_name from USER_LOB_PARTITIONS where table_name='TPAYMENTINTERCHANGE' and partition_name in ('P2015_10', 'P2015_11');

-- test that subpartitioned lob 
  -- assert no subpartitioned lob exist for moved partitions
select subpart.TABLE_NAME, subpart.column_name, subpart.subpartition_name,subpart.LOB_PARTITION_NAME, subpart.tablespace_name   
from USER_LOB_SUBPARTITIONS subpart, USER_LOB_PARTITIONS part where part.table_name='TPAYMENTINTERCHANGE' and subpart.LOB_PARTITION_NAME = part.LOB_PARTITION_NAME
and part.partition_name in ('P2015_10', 'P2015_11');

-- test lob indexes
  -- assert that all supartitioned lob index  are in DORMANT tablespace 
select ind.index_name, ind.status, l.table_name, l.column_name, ind.TABLESPACE_NAME as lindex_tablespace, l.TABLESPACE_NAME as lsegment_tablespace
from USER_IND_PARTITIONS ind inner join USER_LOBS l on ind.index_name = l.INDEX_NAME
where l.table_name  in ('TPAYMENTINTERCHANGEP2015_10','TPAYMENTINTERCHANGEP2015_11'); 



-----------------------------------------------------------------------------------------------------------------
-- Indexes
-----------------------------------------------------------------------------------------------------------------
-- test that all global index are usuable
  -- assert no USUSABLE status, and tablespace of global index is not moved
select status, tablespace_name, ui.* from USER_INDEXES ui where STATUS = 'UNUSABLE';

-- test that partitioned index are in DORMANT (only LOB index here, cause DORMANT table has no normal index)
  -- assert all partitioned indexes are in DORMANT
select pi.index_name, pi.status, pi.partition_name, pi.tablespace_name, ti.table_name
from USER_IND_PARTITIONS pi 
inner join USER_INDEXES ti on pi.index_name = ti.index_name
where ti.table_name in ('TPAYMENTINTERCHANGEP2015_10','TPAYMENTINTERCHANGEP2015_11'); 

-- test that index in source partitions are gone
select pi.index_name, pi.status, pi.partition_name, pi.tablespace_name, ti.table_name
from USER_IND_PARTITIONS pi 
inner join USER_INDEXES ti on pi.index_name = ti.index_name
where pi.partition_name  in ('P2015_10', 'P2015_11');


-- test that no subpartitioned exist in DORMANT
select spi.index_name, spi.status, spi.partition_name, spi.tablespace_name, ti.table_name
from USER_IND_SUBPARTITIONS spi 
inner join USER_INDEXES ti on spi.index_name = ti.index_name
where ti.table_name in ('TPAYMENTINTERCHANGEP2015_10','TPAYMENTINTERCHANGEP2015_11'); 


