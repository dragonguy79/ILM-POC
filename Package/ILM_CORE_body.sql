create or replace PACKAGE BODY ILM_CORE AS
 
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from HOT to WARM
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2WARM_JOB AS
  BEGIN
     FOR managed_table IN (SELECT TABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= CURRENT_MOVE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        CURRENT_MOVE_SEQUENCE := managed_table.MOVESEQUENCE;
        
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.TABLENAME
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TSP) = 1 THEN
            
            -- move all subpartitions of selected partition
            RUN_TASK('ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 100);

            -- move and rebuild subpartitioned index
            RUN_TASK('ILM_CORE.MOVE_REBUILD_SUBPART_INDEX(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS||''')', 200);
            
            -- update tablespace attribute of partition
            RUN_TASK('ILM_CORE.MODIFY_PARTITION_TBS(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 400);
            
            -- update tablespace attribute of partitioned index
            RUN_TASK('ILM_CORE.MODIFY_PARTITION_INDEX_TBS(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||TO_TBS||''')', 500);
          END IF;
    
        END LOOP;

        -- rebuild global index
        RUN_TASK('ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 600);

      END LOOP;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from WARM to COLD
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_WARM2COLD_JOB AS
  BEGIN
     FOR managed_table IN (SELECT TABLENAME, TEMPTABLENAME, COLDTABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= CURRENT_MOVE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        CURRENT_MOVE_SEQUENCE := managed_table.MOVESEQUENCE;
        
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.TABLENAME
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TSP) = 1 THEN
            -- create partition in COLD tables
            RUN_TASK('ILM_CORE.CREATE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME || ''', '||pRow.HIGH_VALUE||')', 100);
            
            -- move all subpartitions of selected partition
            RUN_TASK('ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 200);
            
            -- exchange warm partition with temporary table
            RUN_TASK('ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 300);

            -- exchange cold partition to temporary table
            RUN_TASK('ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 400);
            
            -- move subpartitioned lob
            RUN_TASK('ILM_CORE.MOVE_SUBPARTITIONED_LOB(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''|| ILM_COMMON.GET_TABLESPACE_NAME(HOT_STAGE)|| ''', '''|| TO_TBS||''')', 500);

            -- rebuild subpartitioned index
            RUN_TASK('ILM_CORE.REBUILD_SUBPART_INDEX(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME||''')', 600);
            
            -- drop source partition
            RUN_TASK('ILM_CORE.DROP_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME || ''')', 700);
          END IF;
        END LOOP;
        
        -- rebuild global index of COLD table
        RUN_TASK('ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.COLDTABLENAME || ''')', 800);
        RUN_TASK('ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 900);
        
      END LOOP;
  END;

  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from COLD to DORMANT
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_COLD2DORMANT_JOB AS
    DORMANT_TABLE_NAME VARCHAR2 (30);
  BEGIN
     FOR managed_table IN (SELECT TABLENAME, TEMPTABLENAME, COLDTABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= CURRENT_MOVE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        CURRENT_MOVE_SEQUENCE := managed_table.MOVESEQUENCE;
        
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.COLDTABLENAME
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TSP) = 1 THEN
            
            -- create table in DORMANT tablespace to store partition data 
            DORMANT_TABLE_NAME := SUBSTR(managed_table.TABLENAME, 1, 30 - length(pRow.PARTITION_NAME)) || pRow.PARTITION_NAME;
            IF ILM_COMMON.TABLE_EXIST(DORMANT_TABLE_NAME)=0 THEN
              RUN_TASK('ILM_CORE.COPY_TABLE(''' || managed_table.TEMPTABLENAME || ''', '''||TO_TBS || ''', '''||DORMANT_TABLE_NAME||''')', 100);
            END IF;
        
            -- move all subpartitions of selected partition
            RUN_TASK('ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 200);
            
            -- move subpartitioned lob
            RUN_TASK('ILM_CORE.MOVE_SUBPARTITIONED_LOB(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS||''')', 300);

            -- exchange COLD partition with DORMANT table
            RUN_TASK('ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||DORMANT_TABLE_NAME||''')', 400);
            
            -- drop source partition
            RUN_TASK('ILM_CORE.DROP_PARTITION(''' || managed_table.COLDTABLENAME  || ''', '''||pRow.PARTITION_NAME || ''')', 500);
            
          END IF;
        END LOOP;
        
        -- rebuild global index of COLD table
        RUN_TASK('ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.COLDTABLENAME || ''')', 600);
        
      END LOOP;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Run job
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_JOB(I_JOB VARCHAR2, I_RESUME_JOB_ID in NUMBER DEFAULT NULL) AS
    STEP_ID VARCHAR2 (50);
    OPERATION VARCHAR2(200);
    GET_TBS_STMT VARCHAR2(100) := 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1';
  BEGIN
    -- initialize stages
    IF I_JOB = HOT2WARM_JOB THEN
      FROM_STAGE := HOT_STAGE;
      TO_STAGE := WARM_STAGE;
    ELSIF I_JOB = WARM2COLD_JOB THEN
      FROM_STAGE := WARM_STAGE;
      TO_STAGE := COLD_STAGE;
    ELSIF I_JOB = COLD2DORMANT_JOB THEN
      FROM_STAGE := COLD_STAGE;
      TO_STAGE := DORMANT_STAGE;
    ELSIF I_JOB = HOT2COLD_JOB THEN
      FROM_STAGE := HOT_STAGE;
      TO_STAGE := COLD_STAGE;
    ELSE
      THROW_EXCEPTION('Fail to start job: Job type [' || I_JOB || '] is not valid!');
    END IF;
    
    IF I_RESUME_JOB_ID IS NOT NULL THEN  -- resume job
      IF ILM_COMMON.CAN_RESUME_JOB(I_RESUME_JOB_ID) = 1 THEN
        -- get highest step id from the job
        EXECUTE IMMEDIATE 'SELECT ILMTASK.STEPID, ILMJOB.FROMTABLESPACE, ILMJOB.TOTABLESPACE, ILMlJOB.STARTTIME FROM ILMJOB
                            INNER JOIN ILMTASK on ILMTASK.JOBID = ILMJOB.ID
                            WHERE ILMTASK.ID = (SELECT MAX(ID) FROM ILMTASK WHERE JOBID = :1)'  
            INTO STEP_ID, FROM_TBS, TO_TBS, JOB_START_TSP USING I_RESUME_JOB_ID;
            
        IF (INSTR(STEP_ID, '_') > 0 ) THEN   -- step ID has move sequence and operation id
            CURRENT_MOVE_SEQUENCE := TO_NUMBER(SUBSTR(STEP_ID, 1, INSTR(STEP_ID, '_')-1));
            CURRENT_OPERATION_ID := TO_NUMBER(SUBSTR(STEP_ID, INSTR(STEP_ID, '_') + 1, LENGTH(STEP_ID)));
        ELSE
          CURRENT_OPERATION_ID := TO_NUMBER(STEP_ID);
        END IF;
        
        -- change job status to STARTED
        CURRENT_JOB_ID := I_RESUME_JOB_ID;
        EXECUTE IMMEDIATE 'UPDATE ILMJOB SET STATUS = :1 WHERE ID = :2' USING ILM_CORE.JOBSTATUS_STARTED, CURRENT_JOB_ID;
        LOG_MESSAGE('Resuming existing job [ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
      ELSE
        THROW_EXCEPTION('Job[ID=' || I_RESUME_JOB_ID || '] to resume cannot be found.');
      END IF;
  
    ELSE -- new JOB
      JOB_START_TSP := SYSTIMESTAMP;
      
      -- get source and target tablespace
      FROM_TBS := ILM_COMMON.GET_TABLESPACE_NAME(FROM_STAGE);
      TO_TBS := ILM_COMMON.GET_TABLESPACE_NAME(TO_STAGE, JOB_START_TSP);
      
      CURRENT_MOVE_SEQUENCE := 0;
      CURRENT_OPERATION_ID := 0;
    
      -- create ILMJOB
      SELECT ILMJOB_SEQUENCE.nextval INTO CURRENT_JOB_ID FROM DUAL;
      INSERT INTO ILMJOB(ID, JOBNAME, STATUS, FROMTABLESPACE, TOTABLESPACE, STARTTIME)
      VALUES (CURRENT_JOB_ID, FROM_STAGE ||'2'||TO_STAGE||'_' || TO_CHAR(SYSTIMESTAMP, 'yyyymmdd_HH24miss') , JOBSTATUS_STARTED, FROM_TBS, TO_TBS, JOB_START_TSP);
      LOG_MESSAGE('Created new ILM JOB[ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
    END IF;
  
    -- run operations for designated move
    IF I_JOB = HOT2WARM_JOB THEN
      RUN_HOT2WARM_JOB;
    ELSIF I_JOB = WARM2COLD_JOB THEN
      RUN_WARM2COLD_JOB;
    ELSIF I_JOB = COLD2DORMANT_JOB THEN
      RUN_COLD2DORMANT_JOB;
    ELSIF I_JOB = HOT2COLD_JOB THEN
      RUN_HOT2WARM_JOB;
    END IF;
   
    CURRENT_MOVE_SEQUENCE := null;
    -- job is compoleted
    UPDATE ILMJOB SET STATUS = JOBSTATUS_ENDED, ENDTIME = SYSTIMESTAMP WHERE ID = CURRENT_JOB_ID;
    LOG_MESSAGE('ILM JOB[ID=' || CURRENT_JOB_ID || '] is successfully completed');
    COMMIT;
    
    EXCEPTION
    WHEN OTHERS THEN
      -- update failed job status
      UPDATE ILMJOB SET STATUS=JOBSTATUS_FAILED, ENDTIME=SYSTIMESTAMP WHERE ID=CURRENT_JOB_ID;
      COMMIT;
      raise;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Proxy of all tasks
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, CURRENT_STEP_ID in VARCHAR2) AS
  BEGIN
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
  
  ----------------------------------------------------------------------------------------------------------------
  --  Move all partitions of a table from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_PARTITION(I_TABLE_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '') AS
  BEGIN
    FOR spRow in (select PARTITION_NAME from USER_TAB_PARTITIONS WHERE TABLESPACE_NAME=I_FROM_TBS AND TABLE_NAME=I_TABLE_NAME)
    LOOP
      -- update status in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_PARTITIONMOVE);

      LOG_MESSAGE('Move partition ' || I_TABLE_NAME || '.' || spRow.PARTITION_NAME || ' from tablespace ' || I_FROM_TBS ||' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE PARTITION ' || spRow.PARTITION_NAME || COMPRESSION_CLAUSE || ' TABLESPACE ' || I_TO_TBS || ILM_COMMON.GET_ONLINE_MOVE_CLAUSE() || ILM_COMMON.GET_PARALLEL_CLAUSE();
       
      -- update status in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
    END LOOP;
  END;

  ----------------------------------------------------------------------------------------------------------------
  --  Move all subpartitions of a partition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '') AS
  BEGIN
    FOR spRow in (select SUBPARTITION_NAME from USER_TAB_SUBPARTITIONS WHERE TABLESPACE_NAME=I_FROM_TBS AND PARTITION_NAME=I_PARTITION_NAME)
    LOOP
      -- update status in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_PARTITIONMOVE);
        
      LOG_MESSAGE('Move subpartition ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || ' from tablespace ' || I_FROM_TBS ||' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || COMPRESSION_CLAUSE || ' TABLESPACE ' || I_TO_TBS || ILM_COMMON.GET_ONLINE_MOVE_CLAUSE() || ILM_COMMON.GET_PARALLEL_CLAUSE();
       
      -- update status in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
    END LOOP;
  END;

  ----------------------------------------------------------------------------------------------------------------
  -- Update attribute of partition
    -- Partition does not contain any data since they are stored in subpartitions, but still we need to update the attribute for correctness.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MODIFY_PARTITION_TBS(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '') AS
  BEGIN
    -- update partition metadata [TABLESPACE_NAME]
    LOG_MESSAGE('Modify partition attribute ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || ' to tablespace '|| I_TO_TBS);
    EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || I_PARTITION_NAME || ' ' || COMPRESSION_CLAUSE || ' TABLESPACE ' || I_TO_TBS;
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  -- Move all subpartitioned lobs of a partition from one tablespace to another tablespace
    -- Both LOB segments and indexes are moved together. The indexes are maintained by Oracle and always have USUABLE status.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITIONED_LOB(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2) AS
    COLUMN_NAME varchar2(30);
  BEGIN
    FOR spRow in (
      SELECT subpart.SUBPARTITION_NAME, subpart.COLUMN_NAME 
        from USER_LOB_SUBPARTITIONS subpart inner join USER_LOB_PARTITIONS part on subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME
        WHERE part.PARTITION_NAME=I_PARTITION_NAME AND subpart.TABLESPACE_NAME=I_FROM_TBS AND subpart.TABLE_NAME=I_TABLE_NAME)
    LOOP
      COLUMN_NAME := spRow.COLUMN_NAME;
      -- update metadata in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_LOBMOVE);
    
      LOG_MESSAGE('Move subpartition lob ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || '(column:' || spRow.COLUMN_NAME || ') from tablespace ' || I_FROM_TBS ||' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || ' LOB (' || spRow.COLUMN_NAME || ') STORE AS SECUREFILE (TABLESPACE ' || I_TO_TBS || ')';
      
      -- update metadata in ILMMANAGEDTABLE
      ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
    END LOOP;
    
    -- update partition metadata [TABLESPACE_NAME]
    FOR pRow in (SELECT COLUMN_NAME FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=I_TABLE_NAME AND PARTITION_NAME=I_PARTITION_NAME)
    LOOP
      LOG_MESSAGE('Modify subpartitioned lob attribute ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || '[column: '|| pRow.COLUMN_NAME ||'] to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || I_PARTITION_NAME || ' LOB(' || pRow.COLUMN_NAME || ')(TABLESPACE ' || I_TO_TBS || ')';
    END LOOP;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Rebuild invalid global indexes of a table
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_GLOBAL_INDEX(I_TABLE_NAME in VARCHAR2) AS
  BEGIN
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_INDEXREBUILD);
    
    -- rebuild global index
    FOR iRow in (SELECT INDEX_NAME, TABLESPACE_NAME FROM USER_INDEXES WHERE TABLE_NAME = I_TABLE_NAME AND STATUS in ('UNUSABLE','INVALID'))
    LOOP
      LOG_MESSAGE('Rebuild global index ' || I_TABLE_NAME || '.' || iRow.INDEX_NAME || ' in tablespace ' || iRow.TABLESPACE_NAME);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || iRow.INDEX_NAME || ' REBUILD ' || ILM_COMMON.GET_PARALLEL_CLAUSE() || ' NOLOGGING';
    END LOOP;
    
    -- update metadata in ILMMANAGEDTABLE
    ILM_COMMON.UPDATE_ILMTABLE_STATUS(I_TABLE_NAME, FROM_STAGE, ILMTABLESTATUS_VALID);
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  -- Move all subpartitioned indexes of a sinle partition, from one tablespace to another tablespace, and rebuild them
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_REBUILD_SUBPART_INDEX(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2) AS
  BEGIN
    FOR spRow in (
      SELECT subPart.INDEX_NAME, subPart.SUBPARTITION_NAME 
      from USER_IND_SUBPARTITIONS subPart 
      inner join USER_INDEXES ind on subPart.index_name=ind.index_name
      where ind.table_name=I_TABLE_NAME AND subPart.partition_name=I_PARTITION_NAME AND subPart.TABLESPACE_NAME=I_FROM_TBS)
    LOOP
      LOG_MESSAGE('Rebuild index ' || spRow.INDEX_NAME || ' of subpartition '|| I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || ', and move it to tablespace ' || I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || spRow.INDEX_NAME || ' REBUILD SUBPARTITION '|| spRow.SUBPARTITION_NAME || ' TABLESPACE ' || I_TO_TBS || ILM_COMMON.GET_PARALLEL_CLAUSE();
    END LOOP;
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  -- Update attribute of partition index
    -- Partition does not contain any data since they are stored in subpartitions, but still we need to update the attribute for correctness.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MODIFY_PARTITION_INDEX_TBS(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TO_TBS in VARCHAR2) AS
  BEGIN
    FOR spRow in (
      select part.index_name
      from USER_IND_PARTITIONS part
      inner join USER_INDEXES ind on part.index_name = ind.index_name
      where ind.table_name=I_TABLE_NAME AND part.partition_name = I_PARTITION_NAME)
    LOOP
      LOG_MESSAGE('Change attribute of partitioned index ' || I_TABLE_NAME || '.' || spRow.INDEX_NAME || ' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || spRow.INDEX_NAME  || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || I_PARTITION_NAME || ' TABLESPACE ' || I_TO_TBS;
    END LOOP;
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  -- Rebuild all subpartitioned indexes of a sinle partition, from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_SUBPART_INDEX(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2) AS
  BEGIN
    FOR spRow in (
      SELECT subPart.INDEX_NAME, subPart.SUBPARTITION_NAME 
      FROM USER_IND_SUBPARTITIONS subPart
      WHERE PARTITION_NAME=I_PARTITION_NAME AND STATUS='UNUSABLE') 
    LOOP
      LOG_MESSAGE('Rebuild index ' || spRow.INDEX_NAME || ' of subpartition ' || I_TABLE_NAME || '.'|| spRow.SUBPARTITION_NAME);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || spRow.INDEX_NAME || ' REBUILD SUBPARTITION '|| spRow.SUBPARTITION_NAME || ILM_COMMON.GET_PARALLEL_CLAUSE();
    END LOOP;
  END;
   
  -----------------------------------------------------------------------------------------------------------------
  -- Create a partition in a table at MAXVALUE bound.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE CREATE_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_HIGH_VALUE in TIMESTAMP) AS
    PARTITION_EXIST NUMBER(1) := 0;
  BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=:1 AND PARTITION_NAME=:2' INTO PARTITION_EXIST USING I_TABLE_NAME, I_PARTITION_NAME;
    IF (PARTITION_EXIST=0) THEN
      LOG_MESSAGE('Create a new partition ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || ' with high value ' || I_HIGH_VALUE);
      EXECUTE IMMEDIATE 'ALTER TABLE '||I_TABLE_NAME||' SPLIT PARTITION P9999_12 AT (''' ||I_HIGH_VALUE|| ''') INTO (PARTITION '||I_PARTITION_NAME||', PARTITION P9999_12) '||ILM_COMMON.GET_PARALLEL_CLAUSE();
    END IF;
  END;

  -----------------------------------------------------------------------------------------------------------------
  -- Exchange a partition with corresponding temporary table.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE EXCHANGE_PARTITION(I_PARTITION_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TABLE_NAME in VARCHAR2) AS
    TEMP_TABLE_NAME VARCHAR2(30);
  BEGIN
    LOG_MESSAGE('Exchange partition between ' || I_PARTITION_TABLE_NAME || '.' || I_PARTITION_NAME || ' with ' || I_TABLE_NAME);
    EXECUTE IMMEDIATE 'ALTER TABLE ' || I_PARTITION_TABLE_NAME || ' EXCHANGE PARTITION ' || I_PARTITION_NAME || ' WITH TABLE ' || I_TABLE_NAME || ' WITHOUT VALIDATION';      
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Drop a partition.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE DROP_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2) AS
  BEGIN
    LOG_MESSAGE('Drop partition ' || I_TABLE_NAME|| '.' || I_PARTITION_NAME);
    EXECUTE IMMEDIATE'ALTER TABLE ' || I_TABLE_NAME || ' DROP PARTITION '|| I_PARTITION_NAME || ' UPDATE INDEXES';
  END;

  -----------------------------------------------------------------------------------------------------------------
  -- Create a copy of temporary table in a specific tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE COPY_TABLE(I_TABLE_NAME in VARCHAR2, I_TO_TBS IN VARCHAR2, NEW_TABLE_NAME IN VARCHAR2) AS
    CREATE_STATEMENT VARCHAR2(8000);
    CNT NUMBER;
  BEGIN
    -- set parameter to remove uneeded constraints in DDL generation
    DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
    DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',true);
    DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES', true);
    DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS', false);
    DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS', false);
    CREATE_STATEMENT := DBMS_METADATA.GET_DDL('TABLE', I_TABLE_NAME);

    -- replace table name
    CREATE_STATEMENT := REGEXP_REPLACE(CREATE_STATEMENT, '"'||I_TABLE_NAME||'"', '"'||NEW_TABLE_NAME||'"');
    
    -- replace tablespace
    CREATE_STATEMENT := REGEXP_REPLACE(CREATE_STATEMENT,  'TABLESPACE \"(\S)*"', 'TABLESPACE "' || I_TO_TBS || '"');

    LOG_MESSAGE('Create new table ' || NEW_TABLE_NAME|| ' in tablespace ' || I_TO_TBS);
    EXECUTE IMMEDIATE CREATE_STATEMENT;
  END;
  
END ILM_CORE;