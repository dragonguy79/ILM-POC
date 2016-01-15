SET SERVEROUTPUT ON

-- clean all tablespace and add test value
@../PrepareTablespace.sql;
@../PopulateValue.sql;

-- add test package
@../Package-ILMTEST.sql;
@../PackageBody-ILMTEST.sql;
