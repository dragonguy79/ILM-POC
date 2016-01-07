create or replace PACKAGE ILM_CORE AS 
  -----------------------------------------------------------------------------------------------------------------
  -- Package constant
  -----------------------------------------------------------------------------------------------------------------
  HOT_STAGE CONSTANT VARCHAR2(10) := 'HOT';
  WARM_STAGE CONSTANT VARCHAR2(10) := 'WARM';
  COLD_STAGE CONSTANT VARCHAR2(10) := 'COLD';
  DORMANT_STAGE CONSTANT VARCHAR2(10) := 'DORMANT';
  
  JOBSTATUS_STARTED CONSTANT VARCHAR2(10) := 'STARTED';
  JOBSTATUS_FAILED CONSTANT VARCHAR2(10) := 'FAILED';
  JOBSTATUS_ENDED CONSTANT VARCHAR2(10) := 'ENDED';
  
  TASKSTATUS_STARTED CONSTANT VARCHAR2(10) := 'STARTED';
  TASKSTATUS_FAILED CONSTANT VARCHAR2(10) := 'FAILED';
  TASKSTATUS_ENDED CONSTANT VARCHAR2(10) := 'ENDED';
  
  ILMTABLESTATUS_VALID CONSTANT VARCHAR2(15) := 'VALID';
  ILMTABLESTATUS_PARTITIONMOVE CONSTANT VARCHAR2(15) := 'PARTITION_MOVE';
  ILMTABLESTATUS_INDEXREBUILD CONSTANT VARCHAR2(15) := 'INDEX_REBUILD';
  ILMTABLESTATUS_LOBMOVE CONSTANT VARCHAR2(15) := 'LOB_MOVE';
  
  COMPRESSION_NONE CONSTANT VARCHAR2(30) := 'NONE';
  COMPRESSION_BASIC CONSTANT VARCHAR2(30) := 'BASIC';
  COMPRESSION_OLTP CONSTANT VARCHAR2(30) := 'OLTP';
  COMPRESSION_WAREHOUSE_LOW CONSTANT VARCHAR2(30) := 'WAREHOUSE_LOW';
  COMPRESSION_WAREHOUSE_HIGH CONSTANT VARCHAR2(30) := 'WAREHOUSE_HIGH';
  COMPRESSION_ARCHIVE_LOW CONSTANT VARCHAR2(30) := 'ARCHIVE_LOW';
  COMPRESSION_ARCHIVE_HIGH CONSTANT VARCHAR2(30) := 'ARCHIVE_HIGH';
  
  -----------------------------------------------------------------------------------------------------------------
  -- Package variable
  -----------------------------------------------------------------------------------------------------------------
  FROM_STAGE VARCHAR2(10);
  TO_STAGE VARCHAR2(10);
  
  FROM_TBS VARCHAR2(30);
  TO_TBS VARCHAR2(30);
  
  CURRENT_JOB_ID NUMBER;
  CURRENT_TASK_ID NUMBER;
  CURRENT_MOVE_SEQUENCE NUMBER;
  CURRENT_OPERATION_ID NUMBER;
  
  JOB_START_TSP TIMESTAMP := SYSTIMESTAMP;
  TEXT_BUFFER VARCHAR2(200);
  
  -----------------------------------------------------------------------------------------------------------------
  -- Procedures 
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2WARM_JOB(RESUME_JOB_ID in NUMBER);
  --PROCEDURE RUN_WARM2COLD_JOB(RESUME_JOB_ID in NUMBER);
  -- PROCEDURE RUN_COLD2DORMANT_JOB(RESUME_JOB_ID in NUMBER);
  
  PROCEDURE LOG_MESSAGE (I_MESSAGE in VARCHAR2);
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, OPERATION_ID in NUMBER);
  PROCEDURE MOVE_SUBPARTITIONS(I_TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_GLOBAL_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_PARTITIONED_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_SUBPARTITIONED_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE THROW_EXCEPTION (ERROR_MESSAGE in VARCHAR2);
  PROCEDURE MOVE_LOB_SEGMENTS(I_TABLE_NAME in VARCHAR2);
  
  -----------------------------------------------------------------------------------------------------------------
  -- Functions
  -----------------------------------------------------------------------------------------------------------------

END ILM_CORE;