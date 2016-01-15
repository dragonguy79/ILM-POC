-----------------------------------
-- Setup test
-----------------------------------
@IT-BeforeTest.sql;


-----------------------------------
-- Run test
-----------------------------------
-- run HOT2WARM job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2WARM');
--rollback; 
END;
/
@../UT/UT-HOT2WARM.sql;

-- run WARM2COLD job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'WARM2COLD');
END;
/
-- unit test WARM2COLD job
@../UT/UT-WARM2COLD.sql;

-- run COLD2DORMANT job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'COLD2DORMANT');
END;
/
-- unit test COLD2DORMANT job
@../UT/UT-COLD2DORMANT.sql


-----------------------------------
-- Cleanup test
-----------------------------------
@IT-AfterTest.sql;