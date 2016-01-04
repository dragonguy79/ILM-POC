create or replace PACKAGE BODY ILM_CORE AS
 
  -----------------------------------------------------------------------------------------------------------------
  -- Job to move data from HOT to WARM
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE RUN_HOT2WARM_JOB(RESUME_JOB_ID in NUMBER) AS
    STEP_ID VARCHAR2 (50);
    OPERATION VARCHAR2(200);
  BEGIN
  
    IF RESUME_JOB_ID IS NOT NULL THEN  -- resume job
      IF ILM_COMMON.CAN_RESUME_JOB(RESUME_JOB_ID) = 1 THEN
        -- get highest step id from the job
        EXECUTE IMMEDIATE 'SELECT TASKID, FROMTABLESPACE, TOTABLESPACE FROM ILMTASK WHERE ID = (SELECT MAX(ID) FROM ILMTASK WHERE JOBID = :1)'  
            INTO STEP_ID,FROM_TBS, TO_TBS USING RESUME_JOB_ID;
        IF (INSTR(STEP_ID, '_') > 0 ) THEN
            CURRENT_MOVE_SEQUENCE := TO_NUMBER(SUBSTR(STEP_ID, 1, INSTR(STEP_ID, '_')));
            CURRENT_OPERATION_ID := TO_NUMBER(SUBSTR(STEP_ID, INSTR(STEP_ID, '_') + 1, LENGTH(STEP_ID)));
        ELSE
          CURRENT_OPERATION_ID := TO_NUMBER(STEP_ID);
        END IF;
        
        -- change job status to STARTED
        EXECUTE IMMEDIATE 'UPDATE ILMJOB SET STATUS = ILM_CORE.JOBSTATUS_STARTED WHERE ID = :1' USING 'RESUME_JOB_ID';
      ELSE
        raise_application_error(-20010, 'Fail to start job: ILM job to resume with ID [' || RESUME_JOB_ID || '] does not exist!');
      END IF;
  
    ELSE -- new JOB
      -- check that source and target tablespace exists
      EXECUTE IMMEDIATE 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1' INTO FROM_TBS USING 'HOT_TABLESPACE_NAME';
      EXECUTE IMMEDIATE 'SELECT VALUE FROM ILMCONFIG WHERE PARAM = :1' INTO TO_TBS USING 'WARM_TABLESPACE_NAME';
      
      IF (ILM_COMMON.TABLESPACE_EXIST(FROM_TBS) = 0)  THEN
        raise_application_error(-20010, 'Fail to start job: source tablespace [' || FROM_TBS || '] does not exist!');
      END IF;
      
      IF (ILM_COMMON.TABLESPACE_EXIST(TO_TBS) = 0)  THEN
        raise_application_error(-20010, 'Fail to start job:  target tablespace [' || TO_TBS || '] does not exist!');
      END IF;
    
      -- create ILMJOB
      SELECT ILMJOB_SEQUENCE.nextval INTO CURRENT_JOB_ID FROM DUAL;
      INSERT INTO ILMJOB(ID, JOBNAME, STATUS, FROMTABLESPACE, TOTABLESPACE, STARTTIME)
      VALUES (CURRENT_JOB_ID, 'HOT2WARM_' || TO_CHAR(SYSTIMESTAMP, 'yyyymmdd-HH24miss') , JOBSTATUS_STARTED, FROM_TBS, TO_TBS, SYSTIMESTAMP);
    END IF;
  
   

    /* TODO */
    -- set current task ID 
    
    FOR managed_table IN (SELECT TABLENAME from ILMMANAGEDTABLE WHERE MOVESEQUENCE >= CURRENT_MOVE_SEQUENCE ORDER BY MOVESEQUENCE ASC) 
    LOOP
      OPERATION := 'MOVE_SUBPARTITION(''' || managed_table.TABLENAME || ''')';
      RUN_TASK(OPERATION, 100);
        
       -- move subpartition index
        
        
    END LOOP;
    
    CURRENT_MOVE_SEQUENCE :=  0;
    
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

    -- create a new record in ILMTASK
    INSERT INTO ILMTASK(ID, JOBID, TASKID, STATUS, STARTTIME, ENDTIME) 
    VALUES (ILMTASK_SEQUENCE.nextval, CURRENT_JOB_ID, CURRENT_STEP_ID, TASKSTATUS_STARTED,  SYSTIMESTAMP, null)
    RETURNING ID INTO CURRENT_TASK_ID;
    
    -- run operation
    EXECUTE IMMEDIATE OPERATION;
    
    -- update end time of current task
    UPDATE ILMTASK SET ENDTIME = SYSTIMESTAMP WHERE ID = CURRENT_TASK_ID; 
    
    
    EXCEPTION
    WHEN OTHERS THEN
      -- log error message in table MIGRATION_STEP and MIGRATION_LOG, and raise error
      LOG_MESSAGE(substr('Exception is caught : ' || ' - ' || SQLERRM || ' - ' || dbms_utility.format_error_backtrace() || '. ', 1, 400));
      UPDATE ILMTASK SET STATUS = TASKSTATUS_FAILED, ENDTIME = SYSTIMESTAMP WHERE ID = CURRENT_TASK_ID;
      UPDATE ILMJOB SET STATUS = JOBSTATUS_FAILED, ENDTIME = SYSTIMESTAMP WHERE ID = CURRENT_JOB_ID;
      
      raise;
    
  END;


  -----------------------------------------------------------------------------------------------------------------
  -- Move subpartition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE MOVE_SUBPARTITION(TABLE_NAME in VARCHAR2) AS
  BEGIN
    FOR subpartitionV in (select TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME from USER_TAB_SUBPARTITIONS)
    LOOP
      EXECUTE IMMEDIATE 'DBMS_OUTPUT.PUT_LINE(''TODO'')';
    END LOOP;
  END;
  
  -----------------------------------------------------------------------------------------------------------------
  -- Move subpartition from one tablespace to another tablespace
  -----------------------------------------------------------------------------------------------------------------
  PROCEDURE LOG_MESSAGE (MESSAGE in VARCHAR2) AS
  BEGIN
    INSERT INTO ILMLOG(ID, TASKID, LOG, WHENCREATED) VALUES(ILMLOG_SEQUENCE.nextval, CURRENT_TASK_ID, MESSAGE, SYSTIMESTAMP);
  END;
  

  
END ILM_CORE;
