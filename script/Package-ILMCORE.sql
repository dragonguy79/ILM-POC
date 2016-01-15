create or replace PACKAGE BODY ILM_CORE AS
 
  -----------------------------------------------------------------------------------------------------------------
  -- Run job
    -- I_JOB specifiy type of move. Specify either: HOT2WARM, WARM2COLD, COLD2DORMANT or HOT2COLD
    -- I_RESUME_JOB_ID value is provided to resume an existing job.
    -- Note that a new job cannot be created if previous job of same type did not complete successfully.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_JOB(I_JOB VARCHAR2, I_RESUME_JOB_ID in NUMBER DEFAULT NULL) AS
    I_UNFINISHED_JOB_ID NUMBER;
  BEGIN
    -- initialize stages
    IF I_JOB = HOT2WARM_JOB THEN
      FROM_STAGE:=HOT_STAGE;
      TO_STAGE:=WARM_STAGE;
    ELSIF I_JOB = WARM2COLD_JOB THEN
      FROM_STAGE:=WARM_STAGE;
      TO_STAGE:=COLD_STAGE;
    ELSIF I_JOB = COLD2DORMANT_JOB THEN
      FROM_STAGE:=COLD_STAGE;
      TO_STAGE:=DORMANT_STAGE;
    ELSIF I_JOB = HOT2COLD_JOB THEN
      FROM_STAGE:=HOT_STAGE;
      TO_STAGE:=COLD_STAGE;
    ELSE
      THROW_EXCEPTION('Fail to start job: Job type [' || I_JOB || '] is not valid!');
    END IF;
    
    IF I_RESUME_JOB_ID IS NOT NULL THEN  -- resume job
      IF ILM_COMMON.CAN_RESUME_JOB(I_RESUME_JOB_ID) = 1 THEN
        -- get highest step id from the job
        EXECUTE IMMEDIATE 'SELECT ILMTASK.STEPID, ILMJOB.FROMTABLESPACE, ILMJOB.TOTABLESPACE, ILMJOB.STARTTIME FROM ILMJOB
                            INNER JOIN ILMTASK on ILMTASK.JOBID = ILMJOB.ID
                            WHERE ILMTASK.ID = (SELECT MAX(ID) FROM ILMTASK WHERE JOBID = :1)'  
            INTO RESUME_STEP_ID, FROM_TBS, TO_TBS, JOB_START_TIMESTAMP USING I_RESUME_JOB_ID;
            
        IF RESUME_STEP_ID IS NOT NULL THEN   -- step ID has move sequence and operation id
            RESUME_TABLE_SEQUENCE:=GET_RESUME_TABLE_SEQUENCE(RESUME_STEP_ID);
            RESUME_PARTITION_SEQUENCE:=GET_RESUME_PARTITION_SEQUENCE(RESUME_STEP_ID, FROM_STAGE);
            RESUME_STEP_ID := INCREMENT_COMPLETED_STEP(I_RESUME_JOB_ID, RESUME_STEP_ID);
        END IF;

        -- change job status to STARTED
        CURRENT_JOB_ID:=I_RESUME_JOB_ID;
        EXECUTE IMMEDIATE 'UPDATE ILMJOB SET STATUS = :1 WHERE ID = :2' USING ILM_CORE.JOBSTATUS_STARTED, CURRENT_JOB_ID;
        LOG_MESSAGE('Resuming existing job [ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
      ELSE
        THROW_EXCEPTION('Job[ID=' || I_RESUME_JOB_ID || '] to resume cannot be found.');
      END IF;
  
    ELSE -- new JOB
      I_UNFINISHED_JOB_ID := ILM_COMMON.GET_PREVIOUS_UNFINISHED_JOB(I_JOB);
      
      IF I_UNFINISHED_JOB_ID = 0 THEN
        JOB_START_TIMESTAMP:=SYSTIMESTAMP;
        
        -- get source and target tablespace
        FROM_TBS:=ILM_COMMON.GET_TABLESPACE_NAME(FROM_STAGE);
        TO_TBS:=ILM_COMMON.GET_TABLESPACE_NAME(TO_STAGE, JOB_START_TIMESTAMP);
      
        -- create ILMJOB
        SELECT ILMJOB_SEQUENCE.nextval INTO CURRENT_JOB_ID FROM DUAL;
        INSERT INTO ILMJOB(ID, JOBNAME, STATUS, FROMTABLESPACE, TOTABLESPACE, STARTTIME)
        VALUES (CURRENT_JOB_ID, I_JOB||'_' || TO_CHAR(SYSTIMESTAMP, 'yyyymmdd_HH24miss') , JOBSTATUS_STARTED, FROM_TBS, TO_TBS, JOB_START_TIMESTAMP);
        LOG_MESSAGE('Created new ILM JOB[ID=' || CURRENT_JOB_ID || '] to migrate partitions from '|| FROM_STAGE ||' to ' || TO_STAGE);
      ELSE
        -- found previous non-completed job
        THROW_EXCEPTION('New job of type '|| I_JOB || ' cannot be created because previous job of same type did not end successfully. Please resume previous job with ID ' || I_UNFINISHED_JOB_ID);
      END IF;
    END IF;
  
    -- run operations for designated move
    IF I_JOB = HOT2WARM_JOB THEN
      RUN_HOT2WARM_JOB;
    ELSIF I_JOB = WARM2COLD_JOB THEN
      RUN_WARM2COLD_JOB;
    ELSIF I_JOB = COLD2DORMANT_JOB THEN
      RUN_COLD2DORMANT_JOB;
    ELSIF I_JOB = HOT2COLD_JOB THEN
      RUN_HOT2COLD_JOB;
    END IF;

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
  -- Job to move data from HOT to WARM
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2WARM_JOB AS
    L1_STEP_ID NUMBER;
    L2_STEP_ID NUMBER;
    L3_STEP_ID NUMBER;
  BEGIN
     L1_STEP_ID:=1;
     FOR managed_table IN (SELECT TABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= RESUME_TABLE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        L2_STEP_ID:=1;
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.TABLENAME AND PARTITION_POSITION >= RESUME_PARTITION_SEQUENCE
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TIMESTAMP) = 1 THEN
            
            -- move all subpartitions of selected partition
            L3_STEP_ID:=1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)|| ''', '''||ILM_COMMON.GET_ONLINE_MOVE_CLAUSE()||''')',  
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- move and rebuild subpartitioned index
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_REBUILD_SUBPART_INDEX(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- update tablespace attribute of partition
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.MODIFY_PARTITION_TBS(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- update tablespace attribute of partitioned index
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
            L3_STEP_ID:=L3_STEP_ID+1;
            RUN_TASK(
              'ILM_CORE.MODIFY_PARTITION_INDEX_TBS(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||TO_TBS||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
          END IF;
        END LOOP;

        -- rebuild global index
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));

        -- set ILM table status to valid
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_VALID);
      END LOOP;
      
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from WARM to COLD
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_WARM2COLD_JOB AS
    L1_STEP_ID NUMBER;
    L2_STEP_ID NUMBER;
    L3_STEP_ID NUMBER;
  BEGIN
    L1_STEP_ID:=1;
    FOR managed_table IN (SELECT TABLENAME, TEMPTABLENAME, COLDTABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= RESUME_TABLE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        L2_STEP_ID:=1;
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.TABLENAME AND PARTITION_POSITION >= RESUME_PARTITION_SEQUENCE
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TIMESTAMP) = 1 THEN
            -- create partition in COLD tables
            L3_STEP_ID:=1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.CREATE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME || ''', '||pRow.HIGH_VALUE||')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- move all subpartitions of selected partition
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)|| ''', '''||ILM_COMMON.GET_ONLINE_MOVE_CLAUSE()||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- exchange warm partition with temporary table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- exchange cold partition to temporary table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- move subpartitioned lob
            -- use COLDTABLENAME name because the data partition has been moved to COLD table, so lob partition belongs to COLD table but its tablespace is still in WARM
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_LOBSTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITIONED_LOB(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''|| ILM_COMMON.GET_TABLESPACE_NAME(HOT_STAGE)|| ''', '''|| TO_TBS||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- rebuild subpartitioned index of the moved partition in COLD
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
            RUN_TASK(
              'ILM_CORE.REBUILD_SUBPART_INDEX(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- drop source partition from HOT table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.DROP_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME || ''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
          END IF;
        END LOOP;
        
        -- rebuild global index of COLD table
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.COLDTABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));
        
        -- rebuild global index of HOT table
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));
        
        -- set ILM table status to valid
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_VALID);
      END LOOP;
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from HOT to COLD
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2COLD_JOB AS
    L1_STEP_ID NUMBER;
    L2_STEP_ID NUMBER;
    L3_STEP_ID NUMBER;
  BEGIN
     L1_STEP_ID:=1;
     FOR managed_table IN (SELECT TABLENAME, COLDTABLENAME, TEMPTABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= RESUME_TABLE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        L2_STEP_ID:=1;
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.TABLENAME AND PARTITION_POSITION >= RESUME_PARTITION_SEQUENCE
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TIMESTAMP) = 1 THEN
            
           -- create partition in COLD tables
            L3_STEP_ID:=1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.CREATE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME || ''', '||pRow.HIGH_VALUE||')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- move all subpartitions of selected partition
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)|| ''', '''||ILM_COMMON.GET_ONLINE_MOVE_CLAUSE()||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- exchange hot partition with temporary table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- exchange cold partition to temporary table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||managed_table.TEMPTABLENAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- move subpartitioned lob
            -- use COLDTABLENAME name because the data partition has been moved to COLD table, so lob partition belongs to COLD table but its tablespace is still in HOT
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_LOBSTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITIONED_LOB(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''|| ILM_COMMON.GET_TABLESPACE_NAME(HOT_STAGE)|| ''', '''|| TO_TBS||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- rebuild subpartitioned index of the moved partition in COLD
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
            RUN_TASK(
              'ILM_CORE.REBUILD_SUBPART_INDEX(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- drop source partition from HOT table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.DROP_PARTITION(''' || managed_table.TABLENAME || ''', '''||pRow.PARTITION_NAME || ''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
          END IF;
        END LOOP;

        -- rebuild global index of COLD table
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.COLDTABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));
          
        -- rebuild global index of HOT table
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.TABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));
        
        -- set ILM table status to valid
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_VALID);
      END LOOP;
      
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from COLD to DORMANT
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_COLD2DORMANT_JOB AS
    DORMANT_TABLE_NAME VARCHAR2 (30);
    L1_STEP_ID NUMBER;
    L2_STEP_ID NUMBER;
    L3_STEP_ID NUMBER;
  BEGIN
    L1_STEP_ID:=1;
    FOR managed_table IN (SELECT TABLENAME, TEMPTABLENAME, COLDTABLENAME, MOVESEQUENCE from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= RESUME_TABLE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
      LOOP
        L2_STEP_ID:=1;
        FOR pRow in (
          select PARTITION_NAME, HIGH_VALUE from USER_TAB_PARTITIONS 
          WHERE TABLESPACE_NAME=FROM_TBS AND TABLE_NAME=managed_table.COLDTABLENAME AND PARTITION_POSITION >= RESUME_PARTITION_SEQUENCE
          ORDER BY PARTITION_POSITION)
        LOOP
          -- only work with expired partition
          IF ILM_COMMON.IS_PARTITION_EXPIRED(pRow.HIGH_VALUE, ILM_COMMON.GET_RETENTION(managed_table.TABLENAME, FROM_STAGE), JOB_START_TIMESTAMP) = 1 THEN
            
            -- create table in DORMANT tablespace to store partition data 
            L3_STEP_ID:=1;
            DORMANT_TABLE_NAME:=SUBSTR(managed_table.TABLENAME, 1, 30 - length(pRow.PARTITION_NAME)) || pRow.PARTITION_NAME;
            IF ILM_COMMON.TABLE_EXIST(DORMANT_TABLE_NAME)=0 THEN
              UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
              RUN_TASK(
                'ILM_CORE.COPY_TABLE(''' || managed_table.TEMPTABLENAME || ''', '''||TO_TBS || ''', '''||DORMANT_TABLE_NAME||''')', 
                CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            END IF;
        
            -- move all subpartitions of selected partition
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS|| ''', '''||ILM_COMMON.GET_COMPRESSION_CLAUSE(managed_table.TABLENAME,TO_STAGE)||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- move subpartitioned lob
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_LOBSTALE);
            RUN_TASK(
              'ILM_CORE.MOVE_SUBPARTITIONED_LOB(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||FROM_TBS|| ''', '''||TO_TBS||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));

            -- exchange COLD partition with DORMANT table
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.EXCHANGE_PARTITION(''' || managed_table.COLDTABLENAME || ''', '''||pRow.PARTITION_NAME|| ''', '''||DORMANT_TABLE_NAME||''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
            -- drop source partition
            L3_STEP_ID:=L3_STEP_ID+1;
            UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_DATASTALE);
            RUN_TASK(
              'ILM_CORE.DROP_PARTITION(''' || managed_table.COLDTABLENAME  || ''', '''||pRow.PARTITION_NAME || ''')', 
              CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID, pRow.PARTITION_NAME, L3_STEP_ID));
            
          END IF;
        END LOOP;
        
        -- rebuild global index of COLD table
        L2_STEP_ID:=L2_STEP_ID+1;
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_INDEXSTALE);
        RUN_TASK(
          'ILM_CORE.REBUILD_GLOBAL_INDEX(''' || managed_table.COLDTABLENAME || ''')', 
          CONSTRUCT_STEP_ID(L1_STEP_ID, managed_table.TABLENAME, L2_STEP_ID));
        
        -- set ILM table status to valid
        UPDATE_ILMTABLE_STATUS(managed_table.TABLENAME, FROM_STAGE, ILMTABLESTATUS_VALID);
      END LOOP;
  END;
  

  -----------------------------------------------------------------------------------------------------------------
  -- Get table sequence from step ID. Table sequence is consulted from ILMMANAGED.MOVESEQUENCE.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_RESUME_TABLE_SEQUENCE(I_STEP_ID in VARCHAR2) RETURN NUMBER AS
    MOVE_SEQUENCE NUMBER:=0;
    I_TABLE_NAME VARCHAR2(30);
  BEGIN
    I_TABLE_NAME:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 2);
    IF I_TABLE_NAME IS NOT NULL THEN
       EXECUTE IMMEDIATE 'SELECT MOVESEQUENCE FROM ILMMANAGEDTABLE WHERE TABLENAME=:1' INTO MOVE_SEQUENCE USING I_TABLE_NAME;
    END IF;
    RETURN MOVE_SEQUENCE;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Get partition name from step ID.
    -- Note that step ID contains only HOT table name, so it needs to find out the COLD table name in order to get the right partition sequence.
    -- Partition sequence is consulted from USER_TAB_PARTITIONS.PARTITION_POSITION
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION GET_RESUME_PARTITION_SEQUENCE(I_STEP_ID in VARCHAR2, I_FROM_STAGE in VARCHAR2) RETURN NUMBER AS
    PARTITION_SEQUENCE NUMBER:=0;
    I_TABLE_NAME VARCHAR2(30);
    I_PARTITION_NAME VARCHAR2(30);
  BEGIN
    I_TABLE_NAME:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 2);
    I_PARTITION_NAME:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 4);
    
    IF I_FROM_STAGE = COLD_STAGE THEN
      EXECUTE IMMEDIATE 'SELECT COLDTABLENAME FROM ILMMANAGEDTABLE WHERE TABLENAME=:1' INTO I_TABLE_NAME USING I_TABLE_NAME;
    END IF;
    
    IF I_TABLE_NAME IS NOT NULL AND I_PARTITION_NAME IS NOT NULL THEN
       EXECUTE IMMEDIATE 'SELECT PARTITION_POSITION FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=:1 AND PARTITION_NAME=:2' INTO PARTITION_SEQUENCE USING I_TABLE_NAME, I_PARTITION_NAME;
    END IF;
    RETURN PARTITION_SEQUENCE;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- A step id might indicate a step that has ended successfully. This method returns next step ID if current step is completed,
  -- or it returns current step ID if current step is not yet completed.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION INCREMENT_COMPLETED_STEP(I_JOB_ID in NUMBER, I_STEP_ID in VARCHAR2) RETURN VARCHAR2 AS
    L1_STEP_ID NUMBER;
    TABLE_NAME VARCHAR2(30);
    L2_STEP_ID NUMBER;
    PARTITION_NAME VARCHAR2(30);
    L3_STEP_ID NUMBER;
    I_TASK_STATUS VARCHAR2(30);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT DISTINCT FIRST_VALUE(STATUS)  OVER (ORDER BY ID DESC) FROM ILMTASK WHERE JOBID=:1 AND STEPID=:2' INTO I_TASK_STATUS USING I_JOB_ID, I_STEP_ID;
   
    -- if last step has status ended, then increase step id
    IF I_TASK_STATUS=TASKSTATUS_ENDED THEN
      DECODE_STEP_ID(I_STEP_ID, L1_STEP_ID, TABLE_NAME, L2_STEP_ID, PARTITION_NAME, L3_STEP_ID);
      
      IF L3_STEP_ID IS NOT NULL THEN 
        L3_STEP_ID := L3_STEP_ID + 1;
      ELSIF L2_STEP_ID IS NOT NULL THEN 
        L2_STEP_ID := L2_STEP_ID + 1;
      ELSIF  L1_STEP_ID IS NOT NULL THEN 
        L1_STEP_ID := L1_STEP_ID + 1;
      END IF;
      
      RETURN CONSTRUCT_STEP_ID(L1_STEP_ID, TABLE_NAME, L2_STEP_ID, PARTITION_NAME, L3_STEP_ID);
    END IF;
    
    -- status was not ENDED, continue with existing step id
    RETURN I_STEP_ID;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Construct a step id from subpart of information, which are:
    -- Level 1 step id
    -- Table name
    -- Level 2 step id
    -- Partition name
    -- Level 3 step id
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION CONSTRUCT_STEP_ID(L1_STEP_ID in NUMBER, I_TABLE_NAME in VARCHAR2 default null, L2_STEP_ID in NUMBER default null, I_PARTITION_NAME in VARCHAR default null, L3_STEP_ID in NUMBER default null) RETURN VARCHAR2 AS
  BEGIN
    return L1_STEP_ID || '#' || I_TABLE_NAME || '#' || L2_STEP_ID || '#' || I_PARTITION_NAME || '#' || L3_STEP_ID;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Break a step id into subpart of information, which are:
    -- Level 1 step id
    -- Table name
    -- Level 2 step id
    -- Partition name
    -- Level 3 step id
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE DECODE_STEP_ID(I_STEP_ID in VARCHAR2, L1_STEP_ID out NUMBER, I_TABLE_NAME out VARCHAR2, L2_STEP_ID out NUMBER, I_PARTITION_NAME out VARCHAR, L3_STEP_ID out NUMBER) AS
  BEGIN
    L1_STEP_ID:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 1);
    I_TABLE_NAME:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 2);
    L2_STEP_ID:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 3);
    I_PARTITION_NAME:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 4);
    L3_STEP_ID:=REGEXP_SUBSTR(I_STEP_ID, '[^#]+', 1, 5);
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Permit a step to run if the step is not inferior to resumed step.
    -- because table name and partition name will change, it permit steps if it detects either table or partition has changed.
    -- it checks mainly on Level1, Level2 and Level3 step id with same table name and partition name.
  -----------------------------------------------------------------------------------------------------------------
  FUNCTION PERMIT_STEP_ID(CURRENT_STEP_ID in VARCHAR2, RESUME_STEP_ID in VARCHAR2) RETURN NUMBER AS
    CURRENT_L1_STEP_ID NUMBER;
    CURRENT_I_TABLE_NAME VARCHAR2(30);
    CURRENT_L2_STEP_ID NUMBER;
    CURRENT_I_PARTITION_NAME VARCHAR2(30);
    CURRENT_L3_STEP_ID NUMBER;
    LAST_L1_STEP_ID NUMBER;
    LAST_I_TABLE_NAME VARCHAR2(30);
    LAST_L2_STEP_ID NUMBER;
    LAST_I_PARTITION_NAME VARCHAR2(30);
    LAST_L3_STEP_ID NUMBER;
  BEGIN
    IF RESUME_STEP_ID IS NULL THEN RETURN 1; END IF;
    
    DECODE_STEP_ID(CURRENT_STEP_ID, CURRENT_L1_STEP_ID, CURRENT_I_TABLE_NAME, CURRENT_L2_STEP_ID, CURRENT_I_PARTITION_NAME, CURRENT_L3_STEP_ID);
    DECODE_STEP_ID(RESUME_STEP_ID, LAST_L1_STEP_ID, LAST_I_TABLE_NAME, LAST_L2_STEP_ID, LAST_I_PARTITION_NAME, LAST_L3_STEP_ID);
    
    -- L1 check
    IF LAST_L1_STEP_ID IS NOT NULL THEN
      IF CURRENT_L1_STEP_ID < LAST_L1_STEP_ID 
        THEN RETURN 0; -- inferior L1, forbid
      END IF;
    END IF;
    
    -- L2 check
    IF LAST_I_TABLE_NAME IS NOT NULL THEN
      IF CURRENT_I_TABLE_NAME != LAST_I_TABLE_NAME  -- table has change, new L2 cycle, allow
        THEN RETURN 1;
      ELSIF CURRENT_L2_STEP_ID < LAST_L2_STEP_ID 
        THEN RETURN 0;
      END IF;
    END IF;
    
    -- L3 check
    IF LAST_I_PARTITION_NAME IS NOT NULL THEN
      IF CURRENT_I_PARTITION_NAME != LAST_I_PARTITION_NAME 
        THEN RETURN 1;
      ELSIF CURRENT_L3_STEP_ID < LAST_L3_STEP_ID 
        THEN RETURN 0;
      END IF;
    END IF;
    
    RETURN 1;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Proxy of all tasks
    -- It logs message to ILMLOG
    -- Exeception raised from task is caught and logged
    -- Duration of task execution is logged.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_TASK(OPERATION in VARCHAR2, I_CURRENT_STEP_ID in VARCHAR2) AS
  BEGIN
    -- verfiy step id
    If PERMIT_STEP_ID (I_CURRENT_STEP_ID, RESUME_STEP_ID)=0 THEN
      RETURN;
    END IF;
    CURRENT_STEP_ID:=I_CURRENT_STEP_ID;
  
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
    PREFIX:='[JOB_ID='||CURRENT_JOB_ID||',TASK_ID='||CURRENT_TASK_ID||',STEP_ID='||CURRENT_STEP_ID||']';
    INSERT INTO ILMLOG(ID, MESSAGE, WHENCREATED) VALUES(ILMLOG_SEQUENCE.nextval, SUBSTR(PREFIX || I_MESSAGE, 1, 400), SYSTIMESTAMP);
  END;
  
    
  -----------------------------------------------------------------------------------------------------------------
  -- Update status of tables in ILMMANAGEDTABLE
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_ILMTABLE_STATUS(I_TABLE_NAME in VARCHAR2, I_STAGE in VARCHAR2, I_STATUS in VARCHAR2) AS
  BEGIN
    -- only update if table is provided
    IF (I_TABLE_NAME IS NOT NULL AND I_STAGE IS NOT NULL) THEN
      EXECUTE IMMEDIATE 'UPDATE ILMMANAGEDTABLE SET ' ||
        case I_STAGE when ILM_CORE.HOT_STAGE then 'HOTSTATUS' when ILM_CORE.WARM_STAGE then 'WARMSTATUS' when ILM_CORE.COLD_STAGE then 'COLDSTATUS' else 'IMPOSSIBLE_COLUMN' end || '=:1, 
        LASTMODIFIED=SYSTIMESTAMP 
        WHERE TABLENAME=:2' USING I_STATUS, I_TABLE_NAME;
    END IF;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Throw exception by logging the exception and raise it.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE THROW_EXCEPTION (ERROR_MESSAGE in VARCHAR2) AS
  BEGIN
    LOG_MESSAGE(ERROR_MESSAGE);
    raise_application_error(-20010, ERROR_MESSAGE);
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  --  Move all subpartitions of a partition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '', ONLINE_CLAUSE in VARCHAR2 DEFAULT '') AS
  BEGIN
    FOR spRow in (select SUBPARTITION_NAME from USER_TAB_SUBPARTITIONS WHERE TABLESPACE_NAME=I_FROM_TBS AND PARTITION_NAME=I_PARTITION_NAME)
    LOOP
      LOG_MESSAGE('Move subpartition ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || ' from tablespace ' || I_FROM_TBS ||' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || COMPRESSION_CLAUSE || ' TABLESPACE ' || I_TO_TBS || ONLINE_CLAUSE || ILM_COMMON.GET_PARALLEL_CLAUSE();
    END LOOP;
  END;


  ----------------------------------------------------------------------------------------------------------------
  -- Update table attribute of a partition
    -- Partition does not contain any data since data are stored in subpartitions, but still its attribute is updated for correctness 
    -- in case future subpartitions are created in this partition.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MODIFY_PARTITION_TBS(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TO_TBS in VARCHAR2, COMPRESSION_CLAUSE in VARCHAR2 DEFAULT '') AS
  BEGIN
    -- update partition metadata [TABLESPACE_NAME]
    LOG_MESSAGE('Modify partition attribute ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || ' to tablespace '|| I_TO_TBS);
    EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || I_PARTITION_NAME || ' ' || COMPRESSION_CLAUSE || ' TABLESPACE ' || I_TO_TBS;
  END;
  
  
  ----------------------------------------------------------------------------------------------------------------
  -- Move all subpartitioned lobs from a partition to another tablespace
    -- parameter:
      -- I_TABLE_NAME: Table which the lobs belong to
      -- I_PARTITION_NAME: Data partition name of which its LOBs are to be moved
      -- I_FROM_TBS: Original tablespace of subpartitioned lob to move. Only lobs in this tablespace are moved
      -- I_TO_TBS: Target tablesapce
    -- Both LOB segments and indexes are moved together. LOB indexes are maintained by Oracle and always have USUABLE status.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITIONED_LOB(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_FROM_TBS in VARCHAR2, I_TO_TBS in VARCHAR2) AS
    COLUMN_NAME varchar2(30);
  BEGIN
    FOR spRow in (
      SELECT subpart.SUBPARTITION_NAME, subpart.COLUMN_NAME 
        from USER_LOB_SUBPARTITIONS subpart inner join USER_LOB_PARTITIONS part on subpart.LOB_PARTITION_NAME=part.LOB_PARTITION_NAME
        WHERE part.PARTITION_NAME=I_PARTITION_NAME AND subpart.TABLESPACE_NAME=I_FROM_TBS AND subpart.TABLE_NAME=I_TABLE_NAME)
    LOOP
      COLUMN_NAME:=spRow.COLUMN_NAME;
      
      -- move subpartitioned lob to target tablespace
      LOG_MESSAGE('Move subpartition lob ' || I_TABLE_NAME || '.' || spRow.SUBPARTITION_NAME || '(column:' || spRow.COLUMN_NAME || ') from tablespace ' || I_FROM_TBS ||' to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MOVE SUBPARTITION ' || spRow.SUBPARTITION_NAME || ' LOB (' || spRow.COLUMN_NAME || ') STORE AS SECUREFILE (TABLESPACE ' || I_TO_TBS || ')';
    END LOOP;
    
    -- update partition metadata [TABLESPACE_NAME]
    FOR pRow in (SELECT COLUMN_NAME FROM USER_LOB_PARTITIONS WHERE TABLE_NAME=I_TABLE_NAME AND PARTITION_NAME=I_PARTITION_NAME)
    LOOP
      LOG_MESSAGE('Modify subpartitioned lob attribute ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || '[column: '|| pRow.COLUMN_NAME ||'] to tablespace '|| I_TO_TBS);
      EXECUTE IMMEDIATE 'ALTER TABLE ' || I_TABLE_NAME || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || I_PARTITION_NAME || ' LOB(' || pRow.COLUMN_NAME || ')(TABLESPACE ' || I_TO_TBS || ')';
    END LOOP;
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Rebuild invalid global indexes of a table.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE REBUILD_GLOBAL_INDEX(I_TABLE_NAME in VARCHAR2) AS
  BEGIN
    -- rebuild global index
    FOR iRow in (SELECT INDEX_NAME, TABLESPACE_NAME FROM USER_INDEXES WHERE TABLE_NAME = I_TABLE_NAME AND STATUS in ('UNUSABLE','INVALID'))
    LOOP
      LOG_MESSAGE('Rebuild global index ' || I_TABLE_NAME || '.' || iRow.INDEX_NAME || ' in tablespace ' || iRow.TABLESPACE_NAME);
      EXECUTE IMMEDIATE 'ALTER INDEX ' || iRow.INDEX_NAME || ' REBUILD ' || ILM_COMMON.GET_PARALLEL_CLAUSE() || ' NOLOGGING';
    END LOOP;
  END;
  
  ----------------------------------------------------------------------------------------------------------------
  -- Move all subpartitioned indexes from a partition to another tablespace, and rebuild them.
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
  -- Update table attribute of partition index
    -- Partition does not contain any data since index are stored in subpartitions, but still its attribute is updated for correctness 
    -- in case future subpartitions are created in this partition.
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
  -- Rebuild all subpartitioned indexes of a sinle partition, from one tablespace to another tablespace.
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
  -- Create a partition in a table by spliting the partition at highest bound.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE CREATE_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_HIGH_VALUE in TIMESTAMP) AS
    PARTITION_EXIST NUMBER(1):=0;
    HIGH_BOUND_PARTITION_NAME VARCHAR2(30);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM USER_TAB_PARTITIONS WHERE TABLE_NAME=:1 AND PARTITION_NAME=:2' INTO PARTITION_EXIST USING I_TABLE_NAME, I_PARTITION_NAME;
    -- create the partition only when such partition has not yet existed
    IF (PARTITION_EXIST=0) THEN
      -- get the name of high bound partition
      EXECUTE IMMEDIATE 'SELECT DISTINCT FIRST_VALUE(PARTITION_NAME) OVER (ORDER BY PARTITION_POSITION DESC) FROM USER_TAB_PARTITIONS WHERE table_name=:1' 
        INTO HIGH_BOUND_PARTITION_NAME USING I_TABLE_NAME;
       
      -- split partition
      LOG_MESSAGE('Create a new partition ' || I_TABLE_NAME || '.' || I_PARTITION_NAME || ' with high value ' || I_HIGH_VALUE||', by splitting high bound partition '||HIGH_BOUND_PARTITION_NAME);
      EXECUTE IMMEDIATE 'ALTER TABLE '||I_TABLE_NAME||' SPLIT PARTITION '||HIGH_BOUND_PARTITION_NAME||' AT (''' ||I_HIGH_VALUE|| ''') INTO (PARTITION '||I_PARTITION_NAME||', PARTITION '||HIGH_BOUND_PARTITION_NAME||') '||ILM_COMMON.GET_PARALLEL_CLAUSE();
    END IF;
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Exchange a partition with a table.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE EXCHANGE_PARTITION(I_PARTITION_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2, I_TABLE_NAME in VARCHAR2) AS
    TEMP_TABLE_NAME VARCHAR2(30);
  BEGIN
    LOG_MESSAGE('Exchange partition between ' || I_PARTITION_TABLE_NAME || '.' || I_PARTITION_NAME || ' with ' || I_TABLE_NAME);
    EXECUTE IMMEDIATE 'ALTER TABLE ' || I_PARTITION_TABLE_NAME || ' EXCHANGE PARTITION ' || I_PARTITION_NAME || ' WITH TABLE ' || I_TABLE_NAME || ' WITHOUT VALIDATION';      
  END;
  
  
  -----------------------------------------------------------------------------------------------------------------
  -- Drop a partition from a table.
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE DROP_PARTITION(I_TABLE_NAME in VARCHAR2, I_PARTITION_NAME in VARCHAR2) AS
  BEGIN
    LOG_MESSAGE('Drop partition ' || I_TABLE_NAME|| '.' || I_PARTITION_NAME);
    EXECUTE IMMEDIATE'ALTER TABLE ' || I_TABLE_NAME || ' DROP PARTITION '|| I_PARTITION_NAME || ' UPDATE INDEXES';
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Create a copy of table in a specific tablespace.
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
    CREATE_STATEMENT:=DBMS_METADATA.GET_DDL('TABLE', I_TABLE_NAME);

    -- replace table name
    CREATE_STATEMENT:=REGEXP_REPLACE(CREATE_STATEMENT, '"'||I_TABLE_NAME||'"', '"'||NEW_TABLE_NAME||'"');
    
    -- replace tablespace
    CREATE_STATEMENT:=REGEXP_REPLACE(CREATE_STATEMENT,  'TABLESPACE \"(\S)*"', 'TABLESPACE "' || I_TO_TBS || '"');

    LOG_MESSAGE('Create new table ' || NEW_TABLE_NAME|| ' in tablespace ' || I_TO_TBS);
    EXECUTE IMMEDIATE CREATE_STATEMENT;
  END;
  
END ILM_CORE;
