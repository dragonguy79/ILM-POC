create or replace PACKAGE BODY ILM_COMMON AS

  -----------------------------------------------------------------------------------------------------------------
  -- Check that a tablespace exist
    -- return 0 if tablespace does not exist
    -- return 1 if tablespace found
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION TABLESPACE_EXIST(TBS_NAME in VARCHAR2) RETURN NUMBER AS
    ROW_FOUND NUMBER := 0;
  BEGIN
     SELECT COUNT(*) INTO ROW_FOUND FROM USER_TABLESPACES WHERE TABLESPACE_NAME = UPPER(TBS_NAME) AND ROWNUM <= 1;
     RETURN ROW_FOUND;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Check that a table  exist
    -- return 0 if table does not exist
    -- return 1 if table found
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION TABLE_EXIST(I_TABLE_NAME in VARCHAR2) RETURN NUMBER AS
    ROW_FOUND NUMBER := 0;
  BEGIN
     SELECT COUNT(*) INTO ROW_FOUND FROM USER_TABLES WHERE TABLE_NAME = UPPER(I_TABLE_NAME) AND ROWNUM <= 1;
     RETURN ROW_FOUND;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Check that a job can resumed. A job can be resumed if it did not end completely, and that there is no any more recent run of same job type.
    -- return 0 if the job cannot be resume
    -- return 1 if the job can be resume
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION CAN_RESUME_JOB(I_JOB_ID in NUMBER) RETURN NUMBER AS
    I_JOB_NAME VARCHAR2(50);
    I_STATUS VARCHAR2(10);
    I_JOB_TYPE VARCHAR2(50);
    CNT NUMBER;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT JOBNAME, STATUS FROM ILMJOB WHERE ID=:1' INTO I_JOB_NAME, I_STATUS USING I_JOB_ID;
    
    -- cannot resume job that was completed
    IF I_STATUS = ILM_CORE.JOBSTATUS_ENDED
      THEN RETURN 0;
    END IF;

    -- check that if same job type has existed after the failed job, disallow job 
    I_JOB_TYPE := REGEXP_SUBSTR(I_JOB_NAME, '[^_]+', 1, 1);
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ILMJOB WHERE ID>:1 AND JOBNAME LIKE :2' INTO CNT USING I_JOB_ID, I_JOB_TYPE ||'%';
    IF CNT > 0 
      THEN RETURN 0;
    END IF;

    -- can resume job
    RETURN 1;
    
    -- if no such job type exists before, permit it
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Check that a new job can be created.
    -- return 0 if cannot find previous unfinished job with same job type.
    -- return job ID if previous unfinished job is found.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_PREVIOUS_UNFINISHED_JOB(JOB_TYPE in VARCHAR2) RETURN NUMBER AS
    I_ID NUMBER;
    I_STATUS VARCHAR2(30);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT DISTINCT
       FIRST_VALUE(ID) OVER (ORDER BY ID DESC),
       FIRST_VALUE(STATUS)  OVER (ORDER BY ID DESC)
       FROM ILMJOB WHERE JOBNAME LIKE ''' || JOB_TYPE || '%''' INTO I_ID, I_STATUS;
    
    -- if previous job is unfinished
    IF I_STATUS != ILM_CORE.JOBSTATUS_ENDED THEN 
      RETURN I_ID;
    ELSE 
      RETURN 0;
    END IF;
    
    -- if no such job type exists before, permit it
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
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

    RETURN ' ' ||
    CASE COMPRESSION_TYPE
      WHEN ILM_CORE.COMPRESSION_NONE THEN ' NOCOMPRESS '
      WHEN ILM_CORE.COMPRESSION_BASIC THEN ' COMPRESS BASIC '
      WHEN ILM_CORE.COMPRESSION_OLTP THEN ' COMPRESS FOR OLTP '
      WHEN ILM_CORE.COMPRESSION_WAREHOUSE_LOW THEN ' COMPRESS FOR QUERY LOW '
      WHEN ILM_CORE.COMPRESSION_WAREHOUSE_HIGH THEN ' COMPRESS FOR QUERY HIGH '
      WHEN ILM_CORE.COMPRESSION_ARCHIVE_LOW THEN ' COMPRESS FOR ARCHIVE LOW '
      WHEN ILM_CORE.COMPRESSION_ARCHIVE_HIGH THEN ' COMPRESS FOR ARCHIVE HIGH '
      ELSE ''
    END
    || ' ';
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  --Check if a HIGH VALUE is expired for a specific ILM managed table in specific stage
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION IS_PARTITION_EXPIRED(I_HIGH_VALUE in VARCHAR2, I_RETENTION_MONTH in NUMBER, I_CURRENT_TMP TIMESTAMP) RETURN NUMBER AS
    HIGH_VALUE_T TIMESTAMP;
  BEGIN
      IF I_HIGH_VALUE = 'MAXVALUE' THEN   -- do not process partition with high value=MAXVALUE
        RETURN 0;
      END IF;
      EXECUTE IMMEDIATE 'SELECT '||I_HIGH_VALUE||' FROM DUAL' INTO HIGH_VALUE_T;     -- convert to TIMESTAMP
      
      -- only move subpartitions that are older than retention plan
      IF HIGH_VALUE_T < ADD_MONTHS(I_CURRENT_TMP, -I_RETENTION_MONTH) THEN
        RETURN 1;
      ELSE 
        RETURN 0;
      END IF;
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Return parallel clause based on config parameter.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_PARALLEL_CLAUSE RETURN VARCHAR2 AS
  pValue VARCHAR2(50);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT VALUE FROM ILMCONFIG WHERE PARAM=:1' INTO pValue USING 'PARALLEL_DEGREE';
    
    IF pValue IS NOT NULL 
      THEN RETURN ' PARALLEL '||TO_NUMBER(pValue) || ' ' ;   -- check string value must be numeric
    ELSE  
      RETURN '';
    END IF;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN '';
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Return online clause based on config parameter.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_ONLINE_MOVE_CLAUSE RETURN VARCHAR2 AS
  pValue VARCHAR2(50);
  BEGIN
    SELECT VALUE INTO pValue FROM ILMCONFIG WHERE PARAM='ONLINE_MOVE';
    
    IF UPPER(pValue) = 'TRUE'
      THEN RETURN ' ONLINE ';
    ELSE  
      RETURN '';
    END IF;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN '';
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Return online clause based on config parameter.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_TABLESPACE_NAME(I_STAGE in VARCHAR2, JOB_TIME in TIMESTAMP DEFAULT SYSTIMESTAMP) RETURN VARCHAR2 AS
    GET_TBS_STMT VARCHAR2(100) := 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1';  
    TBS_NAME VARCHAR2(30);
  BEGIN
    IF I_STAGE = ILM_CORE.HOT_STAGE THEN
      EXECUTE IMMEDIATE GET_TBS_STMT INTO TBS_NAME USING 'HOT_TABLESPACE_NAME';
    ELSIF I_STAGE = ILM_CORE.WARM_STAGE THEN
      EXECUTE IMMEDIATE GET_TBS_STMT INTO TBS_NAME USING 'WARM_TABLESPACE_NAME';
    ELSIF I_STAGE = ILM_CORE.COLD_STAGE THEN
      EXECUTE IMMEDIATE GET_TBS_STMT INTO TBS_NAME USING 'COLD_TABLESPACE_NAME';
    ELSIF I_STAGE = ILM_CORE.DORMANT_STAGE THEN
      EXECUTE IMMEDIATE GET_TBS_STMT INTO TBS_NAME USING 'DORMANT_TABLESPACE_NAME';
    ELSE
      raise_application_error(-20010, 'Cannot find tablespace name from stage ' || I_STAGE);
    END IF;
    
    RETURN TBS_NAME;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Return online clause based on config parameter.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_JOB_TIMESTAMP RETURN TIMESTAMP AS
    USER_TIMESTAMP TIMESTAMP;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT TO_TIMESTAMP(VALUE,''DD-MON-RR HH.MI.SSXFF AM'')  FROM ILMCONFIG WHERE PARAM = :1' INTO USER_TIMESTAMP USING 'OVERWRITE_JOB_TIMESTAMP';
    RETURN USER_TIMESTAMP;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN SYSTIMESTAMP;
  END;
  
  
END ILM_COMMON;
