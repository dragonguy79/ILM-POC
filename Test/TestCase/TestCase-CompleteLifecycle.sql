-----------------------------------
-- Setup test
-----------------------------------
@@../BeforeTest.sql;


-----------------------------------
-- Run test
-----------------------------------
-- run HOT2WARM job
BEGIN
  ILM_CORE.CREATE_JOB('HOT2WARM');
END;
/
@@TestHOT2WARM.sql;

-- run WARM2COLD job
BEGIN
  ILM_CORE.CREATE_JOB('WARM2COLD');
END;
/
-- unit test WARM2COLD job
@@TestWARM2COLD.sql;

-- run COLD2DORMANT job
BEGIN
  ILM_CORE.CREATE_JOB('COLD2DORMANT');
END;
/
-- unit test COLD2DORMANT job
@@TestCOLD2DORMANT.sql
