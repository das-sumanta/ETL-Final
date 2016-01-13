/* prestage - drop intermediate insert table */
DROP TABLE if exists dw_prestage.freight_estimate_insert;

/* prestage - create intermediate insert table*/
CREATE TABLE dw_prestage.freight_estimate_insert 
AS
SELECT *
FROM dw_prestage.freight_estimate
WHERE EXISTS (SELECT 1
              FROM (SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                    FROM (SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                          FROM dw_prestage.freight_estimate
                          MINUS
                          SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                          FROM dw_stage.freight_estimate)) a
              WHERE dw_prestage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = a.LANDED_COST_RULE_MATRIX_NZ_ID);

/* prestage - drop intermediate update table*/
DROP TABLE if exists dw_prestage.freight_estimate_update;

/* prestage - create intermediate update table*/
CREATE TABLE dw_prestage.freight_estimate_update 
AS
SELECT decode(SUM(ch_type),
             3,2,
             SUM(ch_type)
       )  ch_type,
       LANDED_COST_RULE_MATRIX_NZ_ID
FROM (SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
             CH_TYPE
      FROM (
     
      
           SELECT LANDED_COST_RULE_MATRIX_NZ_ID,SUBSIDIARY,SUBSIDIARY_ID,'2' CH_TYPE 
           FROM dw_prestage.freight_estimate
      MINUS
      SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
             SUBSIDIARY,
             SUBSIDIARY_ID,
             '2' CH_TYPE
      FROM dw_stage.freight_estimate)
      UNION ALL
      SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
             CH_TYPE
      FROM (
      
      
           SELECT LANDED_COST_RULE_MATRIX_NZ_ID,LANDED_COST_RULE_MATRIX_NZ_NAM,DESCRIPTION,IS_INACTIVE,PERCENT_OF_COST,PLUS_AMOUNT,'1' CH_TYPE 
           FROM dw_prestage.freight_estimate
      MINUS
      SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
             LANDED_COST_RULE_MATRIX_NZ_NAM,
             DESCRIPTION,
             IS_INACTIVE,
             PERCENT_OF_COST,
             PLUS_AMOUNT,
             '1' CH_TYPE
      FROM dw_stage.freight_estimate)) a
WHERE NOT EXISTS (SELECT 1
                  FROM dw_prestage.freight_estimate_insert
                  WHERE dw_prestage.freight_estimate_insert.LANDED_COST_RULE_MATRIX_NZ_ID = a.LANDED_COST_RULE_MATRIX_NZ_ID)
GROUP BY LANDED_COST_RULE_MATRIX_NZ_ID;

/* prestage - drop intermediate delete table*/
DROP TABLE if exists dw_prestage.freight_estimate_delete;

/* prestage - create intermediate delete table*/
CREATE TABLE dw_prestage.freight_estimate_delete 
AS
SELECT *
FROM dw_stage.freight_estimate
WHERE EXISTS (SELECT 1
              FROM (SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                    FROM (SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                          FROM dw_stage.freight_estimate
                          MINUS
                          SELECT LANDED_COST_RULE_MATRIX_NZ_ID
                          FROM dw_prestage.freight_estimate)) a
              WHERE dw_stage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = a.LANDED_COST_RULE_MATRIX_NZ_ID);

/* prestage-> stage*/
SELECT 'no of prestage freight estimate records identified to inserted -->' ||count(1)
FROM dw_prestage.freight_estimate_insert;

/* prestage-> stage*/
SELECT 'no of prestage freight estimate records identified to updated -->' ||count(1)
FROM dw_prestage.freight_estimate_update;

/* prestage-> stage*/
SELECT 'no of prestage freight estimate records identified to deleted -->' ||count(1)
FROM dw_prestage.freight_estimate_delete;

/* stage->delete from stage records to be updated */ 
DELETE
FROM dw_stage.freight_estimate USING dw_prestage.freight_estimate_update
WHERE dw_stage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_update.LANDED_COST_RULE_MATRIX_NZ_ID;

/* stage->delete from stage records which have been deleted */ 
DELETE
FROM dw_stage.freight_estimate USING dw_prestage.freight_estimate_delete
WHERE dw_stage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_delete.LANDED_COST_RULE_MATRIX_NZ_ID;

/* stage->insert into stage records which have been created */ 
INSERT INTO dw_stage.freight_estimate
(
  LANDED_COST_RULE_MATRIX_NZ_ID,
  LANDED_COST_RULE_MATRIX_NZ_NAM,
  DESCRIPTION,
  IS_INACTIVE,
  PERCENT_OF_COST,
  PLUS_AMOUNT,
  SUBSIDIARY_ID,
  SUBSIDIARY,
  DATE_CREATED,
  LAST_MODIFIED_DATE
)
SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
       LANDED_COST_RULE_MATRIX_NZ_NAM,
       DESCRIPTION,
       IS_INACTIVE,
       PERCENT_OF_COST,
       PLUS_AMOUNT,
       SUBSIDIARY_ID,
       SUBSIDIARY,
       DATE_CREATED,
       LAST_MODIFIED_DATE
FROM dw_prestage.freight_estimate_insert;

/* stage->insert into stage records which have been updated */ 
INSERT INTO dw_stage.freight_estimate
(
  LANDED_COST_RULE_MATRIX_NZ_ID,
  LANDED_COST_RULE_MATRIX_NZ_NAM,
  DESCRIPTION,
  IS_INACTIVE,
  PERCENT_OF_COST,
  PLUS_AMOUNT,
  SUBSIDIARY_ID,
  SUBSIDIARY,
  DATE_CREATED,
  LAST_MODIFIED_DATE
)
SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
       LANDED_COST_RULE_MATRIX_NZ_NAM,
       DESCRIPTION,
       IS_INACTIVE,
       PERCENT_OF_COST,
       PLUS_AMOUNT,
       SUBSIDIARY_ID,
       SUBSIDIARY,
       DATE_CREATED,
       LAST_MODIFIED_DATE
FROM dw_prestage.freight_estimate
WHERE EXISTS (SELECT 1
              FROM dw_prestage.freight_estimate_update
              WHERE dw_prestage.freight_estimate_update.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID);

COMMIT;

/* dimention->insert new records in dim freight_estimate */  
INSERT INTO dw.freight_estimate
(
  LANDED_COST_RULE_MATRIX_NZ_ID,
  SHIP_METHOD_NAME,
  SHIP_METHOD_DESCRIPTION,
  IS_INACTIVE,
  PERCENT_OF_COST,
  ADDITIONAL_AMOUNT,
  SUBSIDIARY,
  SUBSIDIARY_ID,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE
)
SELECT LANDED_COST_RULE_MATRIX_NZ_ID,
       NVL(LANDED_COST_RULE_MATRIX_NZ_NAM,'NA_GDW') ,
       NVL(DESCRIPTION,'NA_GDW') ,
       NVL(IS_INACTIVE,'NA_GDW') ,
       PERCENT_OF_COST,
       PLUS_AMOUNT,
       NVL(SUBSIDIARY,'NA_GDW') ,
       NVL(SUBSIDIARY_ID,-99),
       sysdate,
       '9999-12-31 23:59:59',
       'A'
FROM dw_prestage.freight_estimate_insert A;

 /* dimention->update old record as part of SCD2 maintenance*/ 
UPDATE dw.freight_estimate
   SET dw_active = 'I',
       DATE_ACTIVE_TO = (sysdate -1)
WHERE dw_active = 'A'
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1
              FROM dw_prestage.freight_estimate_update
              WHERE dw.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_update.LANDED_COST_RULE_MATRIX_NZ_ID
              AND   dw_prestage.freight_estimate_update.ch_type = 2);

/* dimention->insert the new records as part of SCD2 maintenance*/ 
INSERT INTO dw.freight_estimate
(
  LANDED_COST_RULE_MATRIX_NZ_ID,
  SHIP_METHOD_NAME,
  SHIP_METHOD_DESCRIPTION,
  IS_INACTIVE,
  PERCENT_OF_COST,
  ADDITIONAL_AMOUNT,
  SUBSIDIARY,
  SUBSIDIARY_ID,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE
)
SELECT A.LANDED_COST_RULE_MATRIX_NZ_ID,
       NVL(A.LANDED_COST_RULE_MATRIX_NZ_NAM,'NA_GDW') ,
       NVL(A.DESCRIPTION,'NA_GDW') ,
       NVL(A.IS_INACTIVE,'NA_GDW') ,
       A.PERCENT_OF_COST,
       A.PLUS_AMOUNT,
       NVL(A.SUBSIDIARY,'NA_GDW') ,
       NVL(A.SUBSIDIARY_ID,-99),
       sysdate,
       '9999-12-31 23:59:59',
       'A'
FROM dw_prestage.freight_estimate A
WHERE EXISTS (SELECT 1
              FROM dw_prestage.freight_estimate_update
              WHERE a.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_update.LANDED_COST_RULE_MATRIX_NZ_ID
              AND   dw_prestage.freight_estimate_update.ch_type = 2);

/* dimention->update SCD1 */ 
UPDATE dw.freight_estimate
   SET SHIP_METHOD_NAME = NVL(dw_prestage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_NAM,'NA_GDW'),
       SHIP_METHOD_DESCRIPTION = NVL(dw_prestage.freight_estimate.DESCRIPTION,'NA_GDW'),
       IS_INACTIVE = NVL(dw_prestage.freight_estimate.IS_INACTIVE,'NA_GDW'),
       PERCENT_OF_COST = dw_prestage.freight_estimate.PERCENT_OF_COST,
       ADDITIONAL_AMOUNT = dw_prestage.freight_estimate.PLUS_AMOUNT,
       SUBSIDIARY = NVL(dw_prestage.freight_estimate.SUBSIDIARY,'NA_GDW'),
       SUBSIDIARY_ID = NVL(dw_prestage.freight_estimate.SUBSIDIARY_ID,-99)
FROM dw_prestage.freight_estimate
WHERE dw.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID
AND   EXISTS (SELECT 1
              FROM dw_prestage.freight_estimate_update
              WHERE dw_prestage.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_update.LANDED_COST_RULE_MATRIX_NZ_ID
              AND   dw_prestage.freight_estimate_update.ch_type = 1);

/* dimention->logically delete dw records */ 
UPDATE dw.freight_estimate
   SET DATE_ACTIVE_TO = sysdate -1,
       dw_active = 'I'
FROM dw_prestage.freight_estimate_delete
WHERE dw.freight_estimate.LANDED_COST_RULE_MATRIX_NZ_ID = dw_prestage.freight_estimate_delete.LANDED_COST_RULE_MATRIX_NZ_ID;

COMMIT;

