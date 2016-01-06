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
  -- Get retention number in Month unit for a specific table and ILM stage
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_RETENTION(TABLE_NAME in VARCHAR2, STAGE in VARCHAR2) RETURN NUMBER AS
    RETENTION_MONTH NUMBER(3);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT '|| case STAGE when ILM_CORE.HOT_STAGE then 'HOTRETENTION' when ILM_CORE.WARM_STAGE then 'WARMRETENTION' when ILM_CORE.COLD_STAGE then 'COLDRETENTION' else 'IMPOSSIBLE_COLUMN' end || ' FROM ILMMANAGEDTABLE WHERE TABLENAME = :1' INTO RETENTION_MONTH USING TABLE_NAME;
    RETURN RETENTION_MONTH;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Update status of tables in ILMMANAGEDTABLE
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_ILMTABLE_STATUS(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2, I_STATUS in VARCHAR2) AS
  BEGIN
    EXECUTE IMMEDIATE 'UPDATE ILMMANAGEDTABLE SET '|| case I_STAGE when ILM_CORE.HOT_STAGE then 'HOTSTATUS' when ILM_CORE.WARM_STAGE then 'WARMSTATUS' when ILM_CORE.COLD_STAGE then 'COLDSTATUS' else 'IMPOSSIBLE_COLUMN' end || '=:1, LASTMODIFIED=SYSTIMESTAMP WHERE TABLENAME=:2' USING I_STATUS, I_TABLE_NAME;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Get data compression clause base on setting in ILMMANAGEDTABLE table
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_COMPRESSION_CLAUSE(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2) RETURN VARCHAR2 AS
    COMPRESSION_TYPE VARCHAR2(50);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT ' || 
      case I_STAGE
      when ILM_CORE.WARM_STAGE then 'WARMCOMPRESSION' 
      when ILM_CORE.COLD_STAGE then 'COLDCOMPRESSION' 
      when ILM_CORE.DORMANT_STAGE then 'DORMANTCOMPRESSION'
      else 'IMPOSSIBLE_COLUMN'
      end  || 
      ' FROM ILMMANAGEDTABLE WHERE TABLENAME=:1' INTO COMPRESSION_TYPE USING I_TABLE_NAME;

    RETURN
    CASE COMPRESSION_TYPE
      WHEN ILM_CORE.COMPRESSION_NONE THEN 'NOCOMPRESS'
      WHEN ILM_CORE.COMPRESSION_BASIC THEN 'COMPRESS BASIC'
      WHEN ILM_CORE.COMPRESSION_OLTP THEN 'COMPRESS FOR OLTP'
      WHEN ILM_CORE.COMPRESSION_WAREHOUSE_LOW THEN 'COMPRESS FOR QUERY LOW'
      WHEN ILM_CORE.COMPRESSION_WAREHOUSE_HIGH THEN 'COMPRESS FOR QUERY HIGH'
      WHEN ILM_CORE.COMPRESSION_ARCHIVE_LOW THEN 'COMPRESS FOR ARCHIVE LOW'
      WHEN ILM_CORE.COMPRESSION_ARCHIVE_HIGH THEN 'COMPRESS FOR ARCHIVE HIGH'
      ELSE ''
    END;
  END;
  
END ILM_COMMON;