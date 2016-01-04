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
  
END ILM_COMMON;