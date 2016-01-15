-----------------------------------
-- Setup test
-----------------------------------
@../BeforeTest.sql;


-----------------------------------
-- Run test
-----------------------------------
-- run HOT2COLD job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2COLD');
--rollback; 
END;
/
@TestHOT2COLD.sql;

-- test ILM_COMMON.TABLESPACE_EXIST
DECLARE
  RESULT_VALUE NUMBER;
BEGIN
  RESULT_VALUE := ILM_COMMON.TABLESPACE_EXIST('ILM_HOT_TBS');
  IF RESULT_VALUE != 1 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.TABLESPACE_EXIST failed: cannot find tablespace ILM_HOT_TBS');
  END IF;
  
  RESULT_VALUE := ILM_COMMON.TABLESPACE_EXIST('ILM_NONEXIST_TBS');
  IF RESULT_VALUE != 0 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.TABLESPACE_EXIST failed: found non-existing tablespace ILM_NONEXIST_TBS');
  END IF;
END;
/

-- test ILM_COMMON.TABLE_EXIST
DECLARE
  RESULT_VALUE NUMBER;
BEGIN
  RESULT_VALUE := ILM_COMMON.TABLE_EXIST('ILMJOB');
  IF RESULT_VALUE != 1 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.TABLE_EXIST failed: cannot find table ILMJOB');
  END IF;
  
  RESULT_VALUE := ILM_COMMON.TABLE_EXIST('NOTEXIST');
  IF RESULT_VALUE != 0 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.TABLE_EXIST failed: found non-existing table NOTEXIST');
  END IF;
END;
/


-- test ILM_COMMON.CAN_RESUME_JOB
DECLARE
  RESULT_VALUE NUMBER;
BEGIN
  RESULT_VALUE := ILM_COMMON.CAN_RESUME_JOB(1);
  IF RESULT_VALUE = 1 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.CAN_RESUME_JOB failed:  should not be able to resume job 1');
  END IF;
  
  INSERT INTO ILMJOB(ID, JOBNAME, STATUS, FROMTABLESPACE, TOTABLESPACE, STARTTIME) VALUES (-1, 'HOT2COLD_19000115_173752', 'FAILED', 'FROMTBS', 'TOTBS', CURRENT_TIMESTAMP);
  
  RESULT_VALUE := ILM_COMMON.CAN_RESUME_JOB(-1);
  IF RESULT_VALUE = 1 THEN
   DBMS_OUTPUT.PUT_LINE('ILM_COMMON.CAN_RESUME_JOB failed:  should not be able to resume job -1');
  END IF;
  
  DELETE FROM ILMJOB WHERE ID =-1;
  COMMIT;
END;
/
-----------------------------------
-- Cleanup test
-----------------------------------
@../AfterTest.sql;