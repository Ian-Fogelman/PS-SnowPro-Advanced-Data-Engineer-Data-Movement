-- Snowflake Documentation: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

-- 01 - CREATE THE S3 BUCKET
-- 02 - CREATE THE ROLE IN AWS IAM, ADD THE TRUST POLICY TEMPLATE

/*
##############################
### TRUST POLICY TEMPLATE ####
##############################
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
*/

-- 03 - CREATE THE DATABASE FOR ALL THE COURSE RESOURCES
CREATE DATABASE ps_spadedm;
USE SCHEMA ps_spadedm.public;

-- 04 - CREATE THE STORAGE INTEGRATION
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXXXXXXXXXXX:role/Snowflake_Service2'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ps-spadedm-stage-test/');

-- 04 - CREATE THE STORAGE INTEGRATION
-- USE THE "DESC INTEGRATION COMMAND" GET THE "STORAGE_AWS_EXTERNAL_ID" PROPERY FROM THE DESC COMMAND EXAMPLE: "LHB36066_SFCRole=2_y/NL4IBdqMzFIX+3cAEY/jrgZd4="

DESC INTEGRATION my_s3_integration;

-- 05 - CREATE A STAGE FOR THE S3 BUCKET, USING THE STORAGE INTEGRATION

CREATE STAGE mystage
  URL = 's3://ps-spadedm-stage-test/'
  STORAGE_INTEGRATION = my_s3_integration;

-- LIST THE FILES IN THE STAGE WITH THE LIST COMMAND. THIS WILL PRODUCE AN ERROR:
LIST @mystage;

/*
Error:
User: arn:aws:iam::982534359815:user/62xp0000-s is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::XXXXXXXXXXXX:role/Snowflake_Service
Resolution:
UPDATE THE TRUST RELATIONSHIP IN AWS TO INCLUDE THE "982534359815:user/62xp0000-s" USER (YOUR USER MAY BE DIFFERENT).
THE TRUSTED ENTITIES POLICY SHOULD NOW LOOK LIKE:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::982534359815:user/62xp0000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "LHB36066_SFCRole=2_y/NL4IBdqMzFIX+3cAEY/jrgZd4="
                }
            }
        }
    ]
}
*/

-- ONCE THE TRUSTED ENTITIES POLICY IS UPDATED, ATTEMPT THE LIST STAGE COMMAND AGAIN, THIS TIME YOU SHOULD NOT GET AN ERROR.
LIST @mystage;

-- YOU CAN RUN THE SHOW STAGES COMMAND TO CHECK ALL THE CONFIGURATIONS ON YOUR STAGE. NOTE THE "STORAGE_INTEGRATION" COLUMN HAS A VALUE.
SHOW STAGES;

-- 06 CREATE THE TABLE THAT WILL BE LOADED BY THE SNOWPIPE.
CREATE OR REPLACE TABLE ps_spadedm.public.employee_engagement_survey
(
    employee_id NUMBER(19,0),
    name VARCHAR(16777216),
    gender VARCHAR(20),
    age VARCHAR(16777216),
    department VARCHAR(16777216),
    job_title VARCHAR(16777216),
    satisfaction_score integer,
    work_life_balance_score integer,
    career_growth_score integer,
    communication_score integer,
    teamwork_score integer
);

-- CREATE THE PIPE
CREATE OR REPLACE PIPE ps_spadedm.public.mypipe
  AUTO_INGEST = TRUE
  AS
    COPY INTO ps_spadedm.public.employee_engagement_survey
      FROM @ps_spadedm.public.mystage
      FILE_FORMAT = (type = 'CSV');

-- RUN THE "SHOW PIPES" COMMAND, COPY THE "NOTIFICATION_CHANNEL"
SHOW PIPES;

-- GO BACK TO THE AWS CONSOLE, NAVIGATE TO YOUR S3 BUCKET, PROPERTIES > EVENT NOTIFICATIONS > CREATE EVENT NOTIFICATION
-- ENTER THE NAME "SNOWPIPE_NOTIFY", SUFFIX ".CSV", SELECT "ALL OBJECT CREATE EVENTS", SCROLL TO THE BOTTOM, SELECT "SQS QUEUE"
-- THEN "ENTER SQS QUEUE ARN" AND ADD THE NOTIFICATION_CHANNEL FROM THE SHOW PIPES COMMAND AND CLICK "SAVE"


-- 07 DROP THE "employee_engagement_survey.csv" FILE FROM THE DATA FOLDER OF THE EXERCISE FILES INTO THE S3 BUCKET. PROCESSING CAN TAKE UP TO 1 MINUTE.

-- CHECK THE TABLE FOR DATA     
SELECT * FROM ps_spadedm.public.employee_engagement_survey;

-- CHECK THE MONITORING VIEW
-- SIDE NAVIGATION > "COPY HISTORY" 

-- VIEW ANY ERROR DETAILS
select * from table(validate_pipe_load(
  pipe_name=>'ps_spadedm.public.mypipe',
  start_time=>dateadd(hour, -10, current_timestamp())));
  
-- REPLACE THE PIPE WITH THE CORRECT FILE FORMAT
CREATE OR REPLACE PIPE ps_spadedm.public.mypipe
  AUTO_INGEST = TRUE
  AS
    COPY INTO ps_spadedm.public.employee_engagement_survey
      FROM @ps_spadedm.public.mystage
      FILE_FORMAT = (type = 'CSV' SKIP_HEADER = 1);

-- DOUBLE CHECK THE PIPE DEFINITION WITH THE "SHOW PIPES COMMAND"
SHOW PIPES;

-- DELETE THE EXISTING "EMPLOYEE_ENGAGEMENT_SURVEY" FILE, AND DROP THE "EMPLOYEE_ENGAGEMENT_SURVEY" FILE INTO YOUR S3 BUCKET (AGAIN).
-- WAIT 1 MINUTE

-- THE TABLE SHOULD NOW HAVE DATA, CONFIRMING THAT SNOWPIPE IS CONFIGURED CORRECTLY!
SELECT * FROM ps_spadedm.public.employee_engagement_survey;
