@ILM-Prepare-Tablespace.sql;
@ILM-DDL.sql;
@ILM-Populate-Value.sql;

EXEC EXECUTE IMMEDIATE 'ALTER PACKAGE "' || USER || '"."ILM_CORE" COMPILE BODY';

BEGIN
  ILM_CORE.RUN_JOB(I_JOB => 'HOT2WARM');
--rollback; 
END;
/