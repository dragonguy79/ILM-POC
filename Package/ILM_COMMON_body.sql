create or replace PACKAGE BODY ILM_COMMON AS

  -----------------------------------------------------------------------------------------------------------------
  -- Check that a tablespace does exist
    -- return 0 if tablespace does not exist
    -- return 1 if tablespace found
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION TABLESPACE_EXIST(TBS_NAME in VARCHAR2) RETURN NUMBER AS
    ROW_FOUND NUMBER;
  BEGIN
     SELECT COUNT(*) INTO ROW_FOUND FROM USER_TABLESPACES WHERE TABLESPACE_NAME = TBS_NAME AND ROWNUM <= 1;
     RETURN ROW_FOUND;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Check that a JOB does exist
    -- return 0 if JOB does not exist
    -- return 1 if tablespace found
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION CAN_RESUME_JOB(JOB_ID in NUMBER) RETURN NUMBER AS
    ROW_FOUND NUMBER;
  BEGIN
     SELECT COUNT(*) INTO ROW_FOUND FROM ILMJOB WHERE ID = JOB_ID AND STATUS = ILM_CORE.JOBSTATUS_FAILED AND ROWNUM <= 1;
     RETURN ROW_FOUND;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Check that a JOB does exist
    -- return 0 if JOB does not exist
    -- return 1 if tablespace found
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_RETENTION(TABLE_NAME in VARCHAR2, STAGE in VARCHAR2) RETURN NUMBER AS
    RETENTION_MONTH NUMBER(3);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT '|| case STAGE when ILM_CORE.HOT_STAGE then 'HOTRETENTION' when ILM_CORE.WARM_STAGE then 'WARMRETENTION' when ILM_CORE.COLD_STAGE then 'COLDRETENTION' else 'IMPOSSIBLE_COLUMN' end || ' FROM ILMMANAGEDTABLE WHERE TABLENAME = :1' INTO RETENTION_MONTH USING TABLE_NAME;
    RETURN RETENTION_MONTH;
  END;
  
END ILM_COMMON;