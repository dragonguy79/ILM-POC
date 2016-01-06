-----------------------------------------------
-- Create ILM tables
-----------------------------------------------
DROP table ILMLOG purge;
DROP table ILMTASK purge;
DROP table ILMJOB purge;
DROP table ILMMANAGEDTABLE purge;
DROP table ILMCONFIG purge;

CREATE TABLE "ILMJOB" (	
  "ID" NUMBER(38,0) NOT NULL, 
  "JOBNAME" VARCHAR2(50 CHAR) NOT NULL, 
  "STATUS" VARCHAR2(10 CHAR) NOT NULL, 
  "FROMTABLESPACE" VARCHAR2(30 CHAR) NOT NULL, 
  "TOTABLESPACE" VARCHAR2(30 CHAR) NOT NULL, 
  "STARTTIME" TIMESTAMP NOT NULL,
  "ENDTIME" TIMESTAMP,
  CONSTRAINT ILMJOB_PK PRIMARY KEY (ID)
);

-- TASKID = {movesequence}_{operation}_{attribute}
CREATE TABLE "ILMTASK" (	
  "ID" NUMBER(38,0) NOT NULL,
  "JOBID" NUMBER(38,0) NOT NULL,
  "STEPID" VARCHAR2(50 CHAR) NOT NULL, 
  "STATUS" VARCHAR2(10 CHAR) NOT NULL, 
  "STARTTIME" TIMESTAMP NOT NULL,
  "ENDTIME" TIMESTAMP,
  CONSTRAINT ILMTASK_PK PRIMARY KEY (ID),
  CONSTRAINT FK_ILMTASK_ILMJOB FOREIGN KEY (JOBID) REFERENCES ILMJOB(ID)
);

CREATE TABLE "ILMLOG" (	
  "ID" NUMBER(38,0) NOT NULL, 
	"LOG" VARCHAR2(400 CHAR) NOT NULL, 
  "WHENCREATED" TIMESTAMP  NOT NULL,
  CONSTRAINT ILMLOG_PK PRIMARY KEY (ID)
);


CREATE TABLE "ILMMANAGEDTABLE" (	
  "ID" NUMBER(38,0) NOT NULL, 
  "TABLENAME" VARCHAR2(30 CHAR) NOT NULL, 
  "COLDTABLENAME" VARCHAR2(30 CHAR) NOT NULL, 
  "HOTRETENTION" NUMBER(3,0) NOT NULL,
  "WARMRETENTION" NUMBER(3,0) NOT NULL,
  "COLDRETENTION" NUMBER(3,0) NOT NULL,
  "WARMCOMPRESSION" VARCHAR2(30 CHAR),
  "COLDCOMPRESSION" VARCHAR2(30 CHAR),
  "DORMANTCOMPRESSION" VARCHAR2(30 CHAR),
  "MOVESEQUENCE" NUMBER(4,0) NOT NULL,
  "LASTPARTITIONMOVE" VARCHAR2(50 CHAR),
  "LASTMOVEDATE" TIMESTAMP,
  "STATUS" VARCHAR2(15 CHAR),
  "LASTMODIFIED" TIMESTAMP  NOT NULL,
  CONSTRAINT ILMMANAGEDTABLE_PK PRIMARY KEY (ID),
  CONSTRAINT ILMMANAGEDTABLE_MVSEQ_UNIQ UNIQUE (MOVESEQUENCE)
);


CREATE TABLE "ILMCONFIG" (	
  "ID" NUMBER(38,0) NOT NULL, 
  "PARAM" VARCHAR2(30 CHAR) NOT NULL, 
	"VALUE" VARCHAR2(50 CHAR) NOT NULL, 
  "LASTMODIFIED" TIMESTAMP  NOT NULL,
  CONSTRAINT ILMCONFIG_PK PRIMARY KEY (ID)
);

-----------------------------------------------
-- Create ILM sequences
-----------------------------------------------

DROP SEQUENCE ILMJOB_SEQUENCE;
DROP SEQUENCE ILMTASK_SEQUENCE;
DROP SEQUENCE ILMLOG_SEQUENCE;
DROP SEQUENCE ILMMANAGEDTABLE_SEQUENCE;
DROP SEQUENCE ILMCONFIG_SEQUENCE;

CREATE SEQUENCE "ILMJOB_SEQUENCE" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
CREATE SEQUENCE "ILMTASK_SEQUENCE" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
CREATE SEQUENCE "ILMLOG_SEQUENCE" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
CREATE SEQUENCE "ILMMANAGEDTABLE_SEQUENCE" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
CREATE SEQUENCE "ILMCONFIG_SEQUENCE" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;