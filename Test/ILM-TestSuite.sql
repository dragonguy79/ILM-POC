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

BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2WARM');
--rollback; 
END;
/
@UT-HOT2WARM.sql;

BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'WARM2COLD');
--rollback; 
END;
/
@UT-WARM2COLD.sql;

BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'COLD2DORMANT');
--rollback; 
END;
/
@UT-COLD2DORMANT.sql

--drop package body "ILMTEST"."ILM_TEST";
--drop package "ILMTEST"."ILM_TEST";