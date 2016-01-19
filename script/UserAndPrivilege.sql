-----------------------------------------------
-- User
-----------------------------------------------
-- DROP USER ILMTESTUSER CASCADE;
CREATE USER ILMTESTUSER IDENTIFIED BY password;

-----------------------------------------------
-- Role and Privilege
-----------------------------------------------
-- grant DBA role
GRANT DBA TO ILMTESTUSER;

-- need CREATE TABLE privilege to create tables in DORMANT stage
GRANT CREATE TABLE TO ILMTESTUSER;