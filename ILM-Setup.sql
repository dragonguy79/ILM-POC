-- setup ILM tables
@@script/ILM-DDL.sql;

-- setup package
@@script/Package-ILMCOMMON.sql;
@@script/Package-ILMCORE.sql;

@@script/PackageBody-ILMCOMMON.sql;
@@script/PackageBody-ILMCORE.sql;

-- recompile existing package
EXEC EXECUTE IMMEDIATE 'ALTER PACKAGE "ILM_COMMON" COMPILE BODY';
EXEC EXECUTE IMMEDIATE 'ALTER PACKAGE "ILM_CORE" COMPILE BODY';
