-----------------------------------
-- Setup test
-----------------------------------
@IT-BeforeTest.sql;


-----------------------------------
-- Run test
-----------------------------------
-- run HOT2COLD job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2COLD');
--rollback; 
END;
/
@../UT/UT-HOT2COLD.sql;

-- run COLD2DORMANT job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'COLD2DORMANT');
--rollback; 
END;
/
@../UT/UT-COLD2DORMANT.sql


-----------------------------------
-- Cleanup test
-----------------------------------
@IT-AfterTest.sql;