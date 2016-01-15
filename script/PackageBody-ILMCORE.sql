create or replace PACKAGE ILM_CORE AS 
  -----------------------------------------------------------------------------------------------------------------
  -- Package constant
  -----------------------------------------------------------------------------------------------------------------
  -- type of ILM job
  HOT2WARM_JOB CONSTANT VARCHAR2(15) := 'HOT2WARM';
  WARM2COLD_JOB CONSTANT VARCHAR2(15) := 'WARM2COLD';
  COLD2DORMANT_JOB CONSTANT VARCHAR2(15) := 'COLD2DORMANT';
  HOT2COLD_JOB CONSTANT VARCHAR2(15) := 'HOT2COLD';
  
  -- lifecyle stage
  HOT_STAGE CONSTANT VARCHAR2(10) := 'HOT';
  WARM_STAGE CONSTANT VARCHAR2(10) := 'WARM';
  COLD_STAGE CONSTANT VARCHAR2(10) := 'COLD';
  DORMANT_STAGE CONSTANT VARCHAR2(10) := 'DORMANT';
  
  -- job status
  JOBSTATUS_STARTED CONSTANT VARCHAR2(10) := 'STARTED';
  JOBSTATUS_FAILED CONSTANT VARCHAR2(10) := 'FAILED';
  JOBSTATUS_ENDED CONSTANT VARCHAR2(10) := 'ENDED';
  
  -- task status
  TASKSTATUS_STARTED CONSTANT VARCHAR2(10) := 'STARTED';
  TASKSTATUS_FAILED CONSTANT VARCHAR2(10) := 'FAILED';
  TASKSTATUS_ENDED CONSTANT VARCHAR2(10) := 'ENDED';
  
  -- ILMMANAGEDTABLE status
  ILMTABLESTATUS_VALID CONSTANT VARCHAR2(15) := 'VALID';
  ILMTABLESTATUS_DATASTALE CONSTANT VARCHAR2(15) := 'DATA_STALE';
  ILMTABLESTATUS_LOBSTALE CONSTANT VARCHAR2(15) := 'LOB_STALE';
  ILMTABLESTATUS_INDEXSTALE CONSTANT VARCHAR2(15) := 'INDEX_STALE';
  
  -- compression type
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
  CURRENT_STEP_ID VARCHAR2(100);
  
  RESUME_STEP_ID VARCHAR2(100);
  RESUME_TABLE_SEQUENCE NUMBER := 0;
  RESUME_PARTITION_SEQUENCE NUMBER := 0;
  
  JOB_START_TIMESTAMP TIMESTAMP := SYSTIMESTAMP;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Procedures 
  -----------------------------------------------------------------------------------------------------------------
  -- job
  PROCEDURE RUN_JOB(I_JOB VARCHAR2, I_RESUME_JOB_ID in NUMBER DEFAULT NULL);
  PROCEDURE RUN_HOT2WARM_JOB;
  PROCEDURE RUN_WARM2COLD_JOB;
  PROCEDURE RUN_COLD2DORMANT_JOB;
  PROCEDURE RUN_HOT2COLD_JOB;
  
  -- flow management
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, I_CURRENT_STEP_ID in VARCHAR2);
  PROCEDURE LOG_MESSAGE (I_MESSAGE in VARCHAR2);
  PROCEDURE UPDATE_ILMTABLE_STATUS(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2, I_STATUS in VARCHAR2);
  PROCEDURE THROW_EXCEPTION (ERROR_MESSAGE in VARCHAR2);
  FUNCTION CONSTRUCT_STEP_ID(L1_STEP_ID in NUMBER, I_TABLE_NAME in VARCHAR2 default null, L2_STEP_ID in NUMBER default null, I_PARTITION_NAME in VARCHAR default null, L3_STEP_ID in NUMBER default null) RETURN VARCHAR2;
  PROCEDURE DECODE_STEP_ID(I_STEP_ID in VARCHAR2, L1_STEP_ID out NUMBER, I_TABLE_NAME out VARCHAR2, L2_STEP_ID out NUMBER, I_PARTITION_NAME out VARCHAR, L3_STEP_ID out NUMBER);
  FUNCTION PERMIT_STEP_ID(CURRENT_STEP_ID in VARCHAR2, RESUME_STEP_ID in VARCHAR2) RETURN NUMBER;
  FUNCTION GET_RESUME_TABLE_SEQUENCE(I_STEP_ID in VARCHAR2) RETURN NUMBER;
  FUNCTION GET_RESUME_PARTITION_SEQUENCE(I_STEP_ID in VARCHAR2, I_FROM_STAGE in VARCHAR2) RETURN NUMBER;
  FUNCTION INCREMENT_COMPLETED_STEP(I_JOB_ID in NUMBER, I_STEP_ID in VARCHAR2) RETURN VARCHAR2;
  
  -- data
  PROCEDURE MOVE_SUBPARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '', ONLINE_CLAUSE in VARCHAR2 DEFAULT '');
  PROCEDURE MODIFY_PARTITION_TBS(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT ''); 
  PROCEDURE EXCHANGE_PARTITION(I_PARTITION_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TABLE_NAME in VARCHAR2);
  PROCEDURE CREATE_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_HIGH_VALUE in TIMESTAMP);
  PROCEDURE DROP_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2);
  
  -- lob
  PROCEDURE MOVE_SUBPARTITIONED_LOB(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2);
  
  -- index
  PROCEDURE REBUILD_GLOBAL_INDEX(I_TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_SUBPART_INDEX(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2);
  PROCEDURE MOVE_REBUILD_SUBPART_INDEX(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2);
  PROCEDURE MODIFY_PARTITION_INDEX_TBS(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TO_TBS in VARCHAR2);
  
  PROCEDURE COPY_TABLE(I_TABLE_NAME in VARCHAR2, I_TO_TBS IN VARCHAR2, NEW_TABLE_NAME IN VARCHAR2);
  -----------------------------------------------------------------------------------------------------------------
  -- Functions
  -----------------------------------------------------------------------------------------------------------------

END ILM_CORE;
