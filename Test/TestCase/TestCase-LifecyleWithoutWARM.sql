-----------------------------------
-- Setup test
-----------------------------------
@@../BeforeTest.sql;


-----------------------------------
-- Run test
-----------------------------------
-- run HOT2COLD job
BEGIN
  ILM_CORE.CREATE_JOB('HOT2COLD');
--rollback; 
END;
/
@@TestHOT2COLD.sql;

-- run COLD2DORMANT job
BEGIN
  ILM_CORE.CREATE_JOB('COLD2DORMANT');
--rollback; 
END;
/
@@TestCOLD2DORMANT.sql

