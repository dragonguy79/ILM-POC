SET SERVEROUTPUT ON

-- uncomment lines below to setup or re-initialize test environment. This ensure all ILM tables will be emptied again.
-- @@DestroyTestEnv.sql
-- @@SetupTestEnv.sql;

-- clean all tablespace and add test value
@@PurgeTestValue.sql;
@@PopulateTestValue.sql;

