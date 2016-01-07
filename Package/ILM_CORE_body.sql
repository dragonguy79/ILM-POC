create or replace PACKAGE BODY ILM_CORE AS
 
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from HOT to WARM
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2WARM_JOB(RESUME_JOB_ID in NUMBER) AS
    STEP_ID VARCHAR2 (50);
    OPERATION VARCHAR2(200);
  BEGIN
    FROM_STAGE := HOT_STAGE;
    TO_STAGE := WARM_STAGE;
    
    IF RESUME_JOB_ID IS NOT NULL THEN  -- resume job
      IF ILM_COMMON.CAN_RESUME_JOB(RESUME_JOB_ID) = 1 THEN
        -- get highest step id from the job
        EXECUTE IMMEDIATE 'SELECT ILMTASK.STEPID, ILMJOB.FROMTABLESPACE, ILMJOB.TOTABLESPACE, ILMJOB.STARTTIME FROM ILMJOB
                            INNER JOIN ILMTASK on ILMTASK.JOBID = ILMJOB.ID
                            WHERE ILMTASK.ID = (SELECT MAX(ID) FROM ILMTASK WHERE JOBID = :1)'  
            INTO STEP_ID, FROM_TBS, TO_TBS, JOB_START_TSP USING RESUME_JOB_ID;
            
        IF (INSTR(STEP_ID, '_') > 0 ) THEN   -- step ID has move sequence and operation id
            CURRENT_MOVE_SEQUENCE := TO_NUMBER(SUBSTR(STEP_ID, 1, INSTR(STEP_ID, '_')-1));
            CURRENT_OPERATION_ID := TO_NUMBER(SUBSTR(STEP_ID, INSTR(STEP_ID, '_') + 1, LENGTH(STEP_ID)));
        ELSE
          CURRENT_OPERATION_ID := TO_NUMBER(STEP_ID);
        END IF;
        
        -- change job status to STARTED
        CURRENT_JOB_ID := RESUME_JOB_ID;
        EXECUTE IMMEDIATE 'UPDATE ILMJOB SET STATUS = :1 WHERE ID = :2' USING ILM_CORE.JOBSTATUS_STARTED, CURRENT_JOB_ID;
        LOG_MESSAGE('Resuming existing ILM JOB[ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
      ELSE
        THROW_EXCEPTION('Fail to start job: ILM job to resume with ID [' || RESUME_JOB_ID || '] does not exist!');
      END IF;
  
    ELSE -- new JOB
      -- check that source and target tablespace exists
      EXECUTE IMMEDIATE 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1' INTO FROM_TBS USING 'HOT_TABLESPACE_NAME';
      EXECUTE IMMEDIATE 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1' INTO TO_TBS USING 'WARM_TABLESPACE_NAME';
      
      IF (ILM_COMMON.TABLESPACE_EXIST(FROM_TBS) = 0)  THEN
        THROW_EXCEPTION('Fail to start job: source tablespace [' || FROM_TBS || '] does not exist!');
      END IF;
      
      IF (ILM_COMMON.TABLESPACE_EXIST(TO_TBS) = 0)  THEN
        THROW_EXCEPTION('Fail to start job:  target tablespace [' || TO_TBS || '] does not exist!');
      END IF;
      
      JOB_START_TSP := SYSTIMESTAMP;
      CURRENT_MOVE_SEQUENCE := 0;
      CURRENT_OPERATION_ID := 0;
    
      -- create ILMJOB
      SELECT ILMJOB_SEQUENCE.nextval INTO CURRENT_JOB_ID FROM DUAL;
      INSERT INTO ILMJOB(ID, JOBNAME, STATUS, FROMTABLESPACE, TOTABLESPACE, STARTTIME)
      VALUES (CURRENT_JOB_ID, FROM_STAGE ||'2'||TO_STAGE||'_' || TO_CHAR(SYSTIMESTAMP, 'yyyymmdd-HH24miss') , JOBSTATUS_STARTED, FROM_TBS, TO_TBS, JOB_START_TSP);
      LOG_MESSAGE('Created new ILM JOB[ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
    END IF;
  
    
    FOR managed_table IN (SELECT TABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= CURRENT_MOVE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
    LOOP
      CURRENT_MOVE_SEQUENCE := managed_table.MOVESEQUENCE;
      
      -- move data
      RUN_TASK('ILM_CORE.MOVE_SUBPARTITIONS(''' || managed_table.TABLENAME || ''')', 100);
      RUN_TASK('ILM_CORE.MOVE_LOB_SEGMENTS(''' || managed_table.TABLENAME || ''')', 200);
      
      -- rebuild index
      RUN_TASK('ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 300);
      RUN_TASK('ILM_CORE.REBUILD_PARTITIONED_INDEX(''' || managed_table.TABLENAME || ''')', 400);
      RUN_TASK('ILM_CORE.REBUILD_SUBPARTITIONED_INDEX(''' || managed_table.TABLENAME || ''')', 500);
        
    END LOOP;
    
    CURRENT_MOVE_SEQUENCE := null;
    -- job is compoleted
    UPDATE ILMJOB SET STATUS = JOBSTATUS_ENDED, ENDTIME = SYSTIMESTAMP WHERE ID = CURRENT_JOB_ID;
    COMMIT;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Proxy of all tasks
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, OPERATION_ID in NUMBER) AS
    TASK_ID NUMBER;
    CURRENT_STEP_ID VARCHAR2(50);
  BEGIN
  
    -- skip the operation if current ID is already beyond the operation
    IF CURRENT_OPERATION_ID > OPERATION_ID  THEN
      RETURN;
    END IF;
  
    CURRENT_STEP_ID := CURRENT_MOVE_SEQUENCE || '_' || OPERATION_ID;
    CURRENT_OPERATION_ID := OPERATION_ID;

    -- create a new record in ILMTASK
    INSERT INTO ILMTASK(ID, JOBID, STEPID, STATUS, STARTTIME) 
    VALUES (ILMTASK_SEQUENCE.nextval, CURRENT_JOB_ID, CURRENT_STEP_ID, TASKSTATUS_STARTED,  SYSTIMESTAMP)
    RETURNING ID INTO CURRENT_TASK_ID;
    
    -- run operation
    EXECUTE IMMEDIATE 'BEGIN ' || OPERATION || '; END;';
    
    -- update status and end time of current task upon successful completion
    UPDATE ILMTASK SET STATUS=JOBSTATUS_ENDED, ENDTIME=SYSTIMESTAMP WHERE ID=CURRENT_TASK_ID;
    
    EXCEPTION
    WHEN OTHERS THEN
      -- log error message in table MIGRATION_STEP and MIGRATION_LOG, and raise error
      LOG_MESSAGE('Exception is caught : ' || ' - ' || SQLERRM || ' - ' || dbms_utility.format_error_backtrace() || '. ');
      UPDATE ILMTASK SET STATUS=TASKSTATUS_FAILED, ENDTIME=SYSTIMESTAMP WHERE ID=CURRENT_TASK_ID;
      UPDATE ILMJOB SET STATUS=JOBSTATUS_FAILED, ENDTIME=SYSTIMESTAMP WHERE ID=CURRENT_JOB_ID;
      COMMIT;
      raise;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Log message in ILMLOG table
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE LOG_MESSAGE (I_MESSAGE in VARCHAR2) AS
    PREFIX VARCHAR2(100);
  BEGIN
    PREFIX := '[JOBID='||CURRENT_JOB_ID||',TASKID='||CURRENT_TASK_ID||',MOVESEQUENCE='||CURRENT_MOVE_SEQUENCE||',OPERATIONID='||CURRENT_OPERATION_ID||']';
    INSERT INTO ILMLOG(ID, MESSAGE, WHENCREATED) VALUES(ILMLOG_SEQUENCE.nextval, SUBSTR(PREFIX || I_MESSAGE, 1, 400), SYSTIMESTAMP);
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Proxy of all tasks
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE THROW_EXCEPTION (ERROR_MESSAGE in VARCHAR2) AS
  BEGIN
    LOG_MESSAGE(ERROR_MESSAGE);
    raise_application_error(-20010, ERROR_MESSAGE);
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Operation: Move subpartition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITIONS(I_TABLE_NAME in VARCHAR2) AS
    HIGH_VALUE_C VARCHAR2(100);
    HIGH_VALUE_T TIMESTAMP;
    RETENTION_MONTH NUMBER(3, 0);
  BEGIN
    FOR pRow in (select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=I_TABLE_NAME)
    LOOP

      -- get high value in TIMESTAMP from LONG
      HIGH_VALUE_C := pRow.HIGH_VALUE;  -- convert to VARCHAR2
      IF HIGH_VALUE_C = 'MAXVALUE' THEN   -- do not process partition with high value=MAXVALUE
        CONTINUE;
      END IF;
      EXECUTE IMMEDIATE 'SELECT '||HIGH_VALUE_C||' FROM DUAL' INTO HIGH_VALUE_T;     -- convert to TIMESTAMP

      -- get retention plan
      RETENTION_MONTH := ILM_COMMON.GET_RETENTION(I_TABLE_NAME, FROM_STAGE);
      
      -- only move subpartitions that are older than retention plan
      IF HIGH_VALUE_T < ADD_MONTHS(JOB_START_TSP, -RETENTION_MONTH)  THEN
        -- update metadata in ILMMANAGEDTABLE
        ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_PARTITIONMOVE);
        
        FOR spRow in (select SUBPARTITION_NAME from USER_TAB_SUBPARTITIONS WHERE TABLESPACE_NAME=FROM_TBS AND PARTITION_NAME=pRow.PARTITION_NAME)
        LOOP
          LOG_MESSAGE('Move subpartition ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || ' from tablespace ' || FROM_TBS ||' to tablespace '|| TO_TBS);
          EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || ' ' || ILM_COMMON.GET_COMPRESSION_CLAUSE(I_TABLE_NAME,TO_STAGE) || ' TABLESPACE ' || TO_TBS || ' ONLINE';
        END LOOP;
        
        -- update partition metadata [TABLESPACE_NAME]
        EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || pRow.PARTITION_NAME || ' ' || ILM_COMMON.GET_COMPRESSION_CLAUSE(I_TABLE_NAME,TO_STAGE) || ' TABLESPACE ' || TO_TBS;
      END IF;

      -- validate status of ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
    END LOOP;
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Operation: Rebuild global index
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_GLOBAL_INDEX(TABLE_NAME in VARCHAR2) AS
  BEGIN
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_INDEXREBUILD);
    
    -- rebuild global index
    FOR iRow in (SELECT INDEX_NAME, TABLESPACE_NAME FROM USER_INDEXES WHERE TABLE_NAME = TABLE_NAME AND STATUS in ('UNUSABLE','INVALID'))
    LOOP
      LOG_MESSAGE('Rebuild global index ' || TABLE_NAME || '.' || iRow.INDEX_NAME || ' in tablespace ' || iRow.TABLESPACE_NAME);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || iRow.INDEX_NAME || ' REBUILD NOLOGGING';
    END LOOP;
    
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Operation: Move and rebuild local partitioned index
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_PARTITIONED_INDEX(TABLE_NAME in VARCHAR2) AS
  BEGIN
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_INDEXREBUILD);
    
    -- move and rebuild 1-level partitioned index
    FOR iRow in (SELECT INDEX_NAME, PARTITION_NAME FROM USER_IND_PARTITIONS WHERE TABLE_NAME = TABLE_NAME AND ((TABLESPACE_NAME = FROM_TBS AND STATUS!='N/A') OR (TABLESPACE_NAME = TO_TBS AND STATUS='UNUSABLE')) ) 
    LOOP
      LOG_MESSAGE('Rebuild partition ' || iRow.PARTITION_NAME || ' index ' || TABLE_NAME || '.' || iRow.INDEX_NAME);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || iRow.INDEX_NAME || ' REBUILD PARTITION '|| iRow.PARTITION_NAME || ' TABLESPACE ' || TO_TBS;
    END LOOP;
    
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
  END;

 -----------------------------------------------------------------------------------------------------------------
  -- Operation: Move and rebuild local subpartitioned index
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_SUBPARTITIONED_INDEX(TABLE_NAME in VARCHAR2) AS
    HIGH_VALUE_C VARCHAR2(100);
    HIGH_VALUE_T TIMESTAMP;
    RETENTION_MONTH NUMBER(3, 0);
  BEGIN
    FOR pRow in (select part.PARTITION_NAME, part.HIGH_VALUE, part.INDEX_NAME from USER_IND_PARTITIONS part INNER JOIN USER_INDEXES ind on ind.INDEX_NAME=part.INDEX_NAME WHERE part.TABLESPACE_NAME=FROM_TBS AND ind.TABLE_NAME=TABLE_NAME ORDER BY part.INDEX_NAME)
    LOOP
     
      -- get high value in TIMESTAMP from LONG
      HIGH_VALUE_C := pRow.HIGH_VALUE;  -- convert to VARCHAR2
      IF HIGH_VALUE_C = 'MAXVALUE' THEN   -- do not process partition with high value=MAXVALUE
        CONTINUE;
      END IF;
      EXECUTE IMMEDIATE 'SELECT '||HIGH_VALUE_C||' FROM DUAL' INTO HIGH_VALUE_T;     -- convert to TIMESTAMP
    
      -- get retention plan
      RETENTION_MONTH := ILM_COMMON.GET_RETENTION(TABLE_NAME, FROM_STAGE);
      
      -- only move subpartitioned index that are older than retention plan
      IF HIGH_VALUE_T < ADD_MONTHS(JOB_START_TSP, -RETENTION_MONTH)  THEN
      
        -- update metadata in ILMMANAGEDTABLE
        ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_INDEXREBUILD);
        
        -- move and rebuild 2-level subpartitioned index
        FOR spRow in (SELECT INDEX_NAME, SUBPARTITION_NAME FROM USER_IND_SUBPARTITIONS WHERE PARTITION_NAME=pRow.PARTITION_NAME AND (TABLESPACE_NAME=FROM_TBS OR (TABLESPACE_NAME=TO_TBS AND STATUS='UNUSABLE') )) 
        LOOP
          LOG_MESSAGE('Rebuild subpartition ' || spRow.SUBPARTITION_NAME || ' index ' || TABLE_NAME || '.' || spRow.INDEX_NAME);
          EXECUTE IMMEDIATE 'ALTER INDEX ' || spRow.INDEX_NAME || ' REBUILD SUBPARTITION '|| spRow.SUBPARTITION_NAME || ' TABLESPACE ' || TO_TBS;
        END LOOP;
        
        -- update partition metadata [TABLESPACE_NAME]
        EXECUTE IMMEDIATE 'ALTER INDEX ' || pRow.INDEX_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || pRow.PARTITION_NAME || ' TABLESPACE ' || TO_TBS;
      END IF;
    
    END LOOP;
    
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Operation: Move subpartition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_LOB_SEGMENTS(I_TABLE_NAME in VARCHAR2) AS
    HIGH_VALUE_C VARCHAR2(100);
    HIGH_VALUE_T TIMESTAMP;
    RETENTION_MONTH NUMBER(3, 0);
  BEGIN
    FOR pRow in (
      select lob.lob_partition_name, tab.HIGH_VALUE, lob.column_name, lob.partition_name
        from USER_TAB_PARTITIONS tab INNER JOIN USER_LOB_PARTITIONS lob on lob.partition_name=tab.partition_name
        WHERE tab.TABLE_NAME=I_TABLE_NAME
    )
    LOOP

      -- get high value in TIMESTAMP from LONG
      HIGH_VALUE_C := pRow.HIGH_VALUE;  -- convert to VARCHAR2
      IF HIGH_VALUE_C = 'MAXVALUE' THEN   -- do not process partition with high value=MAXVALUE
        CONTINUE;
      END IF;
      EXECUTE IMMEDIATE 'SELECT '||HIGH_VALUE_C||' FROM DUAL' INTO HIGH_VALUE_T;     -- convert to TIMESTAMP

      -- get retention plan
      RETENTION_MONTH := ILM_COMMON.GET_RETENTION(I_TABLE_NAME, FROM_STAGE);
      
      -- only move subpartitions that are older than retention plan
      IF HIGH_VALUE_T < ADD_MONTHS(JOB_START_TSP, -RETENTION_MONTH)  THEN
        -- update metadata in ILMMANAGEDTABLE
        ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_LOBMOVE);
        
        -- move subpartitioned lob
        FOR spRow in (
          select SUBPARTITION_NAME from USER_LOB_SUBPARTITIONS 
            WHERE LOB_PARTITION_NAME=pRow.LOB_PARTITION_NAME AND TABLESPACE_NAME=FROM_TBS)
        LOOP
          LOG_MESSAGE('Move subpartition lob ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || '(column:' || pRow.COLUMN_NAME || ') from tablespace ' || FROM_TBS ||' to tablespace '|| TO_TBS);
          EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || ' LOB (' || pRow.COLUMN_NAME || ') STORE AS SECUREFILE (TABLESPACE ' || TO_TBS || ')';
        END LOOP;
        
        -- update partition metadata [TABLESPACE_NAME]
        EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || pRow.PARTITION_NAME || ' LOB(' || pRow.COLUMN_NAME || ')(TABLESPACE ' || TO_TBS || ')';
        
      END IF;
      
      -- update metadata in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
    END LOOP;
  END;
  
END ILM_CORE;