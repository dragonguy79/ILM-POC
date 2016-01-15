SET SERVEROUTPUT ON

-- clean all tablespace and add test value
@ILM-Prepare-Tablespace.sql;
@../ILM-DDL.sql;
@ILM-Populate-Value.sql;

-- recompile existing package
EXEC EXECUTE IMMEDIATE 'ALTER PACKAGE "' || USER || '"."ILM_CORE" COMPILE BODY';

-- add test package
--@ILM_TEST_PACKAGE.sql;
--@ILM_TEST_PACKAGE_body.sql;

-- run HOT2COLD job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2COLD');
--rollback; 
END;
/
@UT-HOT2COLD.sql;

-- run COLD2DORMANT job
BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'COLD2DORMANT');
--rollback; 
END;
/
@UT-COLD2DORMANT.sql

--drop package body "ILMTEST"."ILM_TEST";
--drop package "ILMTEST"."ILM_TEST";