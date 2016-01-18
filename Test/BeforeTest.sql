SET SERVEROUTPUT ON

-- clean all tablespace and add test value
@@PurgeTestValue.sql;
@@PrepareTablespace.sql;
@@SetupTestValue.sql;

-- add test package
@@Package-ILMTEST.sql;
@@PackageBody-ILMTEST.sql;
