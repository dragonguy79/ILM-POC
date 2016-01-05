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
  ILMTABLESTATUS_INCONSISTENT CONSTANT VARCHAR2(15) := 'INCONSISTENT';
  ILMTABLESTATUS_INDEXREBUILD CONSTANT VARCHAR2(15) := 'INDEX_REBUILD';
  
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
  
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, OPERATION_ID in NUMBER);
  PROCEDURE MOVE_SUBPARTITIONS(TABLE_NAME in VARCHAR2);
  PROCEDURE LOG_MESSAGE (MESSAGE in VARCHAR2);
  PROCEDURE REBUILD_GLOBAL_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_PARTITIONED_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE REBUILD_SUBPARTITIONED_INDEX(TABLE_NAME in VARCHAR2);
  PROCEDURE THROW_EXCEPTION (ERROR_MESSAGE in VARCHAR2);
  
  -----------------------------------------------------------------------------------------------------------------
  -- Functions
  -----------------------------------------------------------------------------------------------------------------

END ILM_CORE;