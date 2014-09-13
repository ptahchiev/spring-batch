
-- create the requisite table

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS  (
	JOB_EXECUTION_ID BIGINT NOT NULL ,
	TYPE_CD VARCHAR(6) NOT NULL ,
	KEY_NAME VARCHAR(100) NOT NULL ,
	STRING_VAL VARCHAR(250) ,
	DATE_VAL DATETIME DEFAULT NULL ,
	LONG_VAL BIGINT ,
	DOUBLE_VAL DOUBLE PRECISION ,
	IDENTIFYING CHAR(1) NOT NULL ,
	constraint JOB_EXEC_PARAMS_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

-- insert script that 'copies' existing batch_job_params to batch_job_execution_params
-- sets new params to identifying ones
-- verified on h2, 

INSERT INTO BATCH_JOB_EXECUTION_PARAMS 
	( JOB_EXECUTION_ID , TYPE_CD, KEY_NAME, STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING )
SELECT 
	JE.JOB_EXECUTION_ID , JP.TYPE_CD , JP.KEY_NAME , JP.STRING_VAL , JP.DATE_VAL , JP.LONG_VAL , JP.DOUBLE_VAL , 'Y' 
FROM 
	BATCH_JOB_PARAMS JP,BATCH_JOB_EXECUTION JE
WHERE 
	JP.JOB_INSTANCE_ID = JE.JOB_INSTANCE_ID;