create or replace PACKAGE ILM_COMMON AS 

  FUNCTION TABLESPACE_EXIST(TBS_NAME in VARCHAR2) RETURN NUMBER;
  FUNCTION CAN_RESUME_JOB(JOB_ID in NUMBER) RETURN NUMBER;
  FUNCTION GET_RETENTION(TABLE_NAME in VARCHAR2, STAGE in VARCHAR2) RETURN NUMBER;
  FUNCTION GET_COMPRESSION_CLAUSE(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2) RETURN VARCHAR2;
  FUNCTION IS_PARTITION_EXPIRED(I_HIGH_VALUE in VARCHAR2, I_RETENTION_MONTH in NUMBER, I_CURRENT_TMP TIMESTAMP) RETURN NUMBER;
  FUNCTION GET_PARALLEL_CLAUSE RETURN VARCHAR2;
  FUNCTION GET_ONLINE_MOVE_CLAUSE RETURN VARCHAR2;

  PROCEDURE UPDATE_ILMTABLE_STATUS(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2, I_STATUS in VARCHAR2);
  
END ILM_COMMON;