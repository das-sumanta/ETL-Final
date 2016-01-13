/* prestage - drop intermediate insert table */ 
DROP TABLE if exists dw_prestage.territories_insert;

/* prestage - create intermediate insert table*/ 
CREATE TABLE dw_prestage.territories_insert 
AS
SELECT *
FROM dw_prestage.territories
WHERE EXISTS (SELECT 1
              FROM (SELECT territory_id
                    FROM (SELECT territory_id
                          FROM dw_prestage.territories
                          MINUS
                          SELECT territory_id
                          FROM dw_stage.territories)) a
              WHERE dw_prestage.territories.territory_id = a.territory_id);

/* prestage - drop intermediate update table*/ 
DROP TABLE if exists dw_prestage.territories_update;

/* prestage - create intermediate update table*/ 
CREATE TABLE dw_prestage.territories_update 
AS
SELECT decode(SUM(ch_type),
             3,2,
             SUM(ch_type)
       )  ch_type,
       territory_id
FROM (SELECT territory_id,
             CH_TYPE
      FROM (SELECT territory_id,
                   IS_INACTIVE,
                   '2' CH_TYPE
            FROM dw_prestage.territories
            MINUS
            SELECT territory_id,
                   IS_INACTIVE,
                   '2' CH_TYPE
            FROM dw_stage.territories)
      UNION ALL
      SELECT territory_id,
             CH_TYPE
      FROM (SELECT territory_id
                   ,TERRITORY
                   ,SUBSIDIARY
                   ,SUBSIDIARY_ID
                   ,'1' CH_TYPE
            FROM dw_prestage.territories
            MINUS
            SELECT territory_id
                   ,TERRITORY
                   ,SUBSIDIARY
                   ,SUBSIDIARY_ID
                   ,'1' CH_TYPE
            FROM dw_stage.territories)) a
WHERE NOT EXISTS (SELECT 1
                  FROM dw_prestage.territories_insert
                  WHERE dw_prestage.territories_insert.territory_id = a.territory_id)
GROUP BY territory_id;

/* prestage - drop intermediate delete table*/ 
DROP TABLE if exists dw_prestage.territories_delete;

/* prestage - create intermediate delete table*/ 
CREATE TABLE dw_prestage.territories_delete 
AS
SELECT *
FROM dw_stage.territories
WHERE EXISTS (SELECT 1
              FROM (SELECT territory_id
                    FROM (SELECT territory_id
                          FROM dw_stage.territories
                          MINUS
                          SELECT territory_id
                          FROM dw_prestage.territories)) a
              WHERE dw_stage.territories.territory_id = a.territory_id);

/* prestage-> stage*/ 
SELECT 'no of prestage territory records identified to inserted -->' ||count(1)
FROM dw_prestage.territories_insert;

/* prestage-> stage*/ 
SELECT 'no of prestage territory records identified to updated -->' ||count(1)
FROM dw_prestage.territories_update;

/* prestage-> stage*/ 
SELECT 'no of prestage territory records identified to deleted -->' ||count(1)
FROM dw_prestage.territories_delete;

/* stage -> delete from stage records to be updated */ 
DELETE
FROM dw_stage.territories USING dw_prestage.territories_update
WHERE dw_stage.territories.territory_id = dw_prestage.territories_update.territory_id;

/* stage -> delete from stage records which have been deleted */ 
DELETE
FROM dw_stage.territories USING dw_prestage.territories_delete
WHERE dw_stage.territories.territory_id = dw_prestage.territories_delete.territory_id;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.territories
(
  TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
)
SELECT TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
FROM dw_prestage.territories_insert;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.territories
(
  TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
)
SELECT TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
FROM dw_prestage.territories
WHERE EXISTS (SELECT 1
              FROM dw_prestage.territories_update
              WHERE dw_prestage.territories_update.territory_id = dw_prestage.territories.territory_id);

COMMIT;

/* dimension ->insert new records in dim territories */ 
INSERT INTO dw.territories
(
  TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
  ,DATE_ACTIVE_FROM
  ,DATE_ACTIVE_TO
  ,DW_ACTIVE
)
SELECT A.territory_id,
       NVL(A.TERRITORY,'NA_GDW'),
       NVL(A.SUBSIDIARY,'NA_GDW'),
       NVL(A.SUBSIDIARY_ID,-99),
       NVL(A.IS_INACTIVE,'NA_GDW'),
       sysdate,
       '9999-12-31 23:59:59',
       'A'
FROM dw_prestage.territories_insert A;

/* dimension -> update old record as part of SCD2 maintenance*/ 
UPDATE dw.territories
   SET dw_active = 'I',
       DATE_ACTIVE_TO = (sysdate -1)
WHERE dw_active = 'A'
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1
              FROM dw_prestage.territories_update
              WHERE dw.territories.territory_id = dw_prestage.territories_update.territory_id
              AND   dw_prestage.territories_update.ch_type = 2);

/* dimension -> insert the new records as part of SCD2 maintenance*/ 
INSERT INTO dw.territories
(
  TERRITORY_ID
  ,TERRITORY
  ,SUBSIDIARY
  ,SUBSIDIARY_ID
  ,IS_INACTIVE
  ,DATE_ACTIVE_FROM
  ,DATE_ACTIVE_TO
  ,DW_ACTIVE
)
SELECT A.territory_id,
       NVL(A.TERRITORY,'NA_GDW'),
       NVL(A.SUBSIDIARY,'NA_GDW'),
       NVL(A.SUBSIDIARY_ID,-99),
       NVL(A.IS_INACTIVE,'NA_GDW'),
       sysdate,
       '9999-12-31 23:59:59',
       'A'
FROM dw_prestage.territories A
WHERE EXISTS (SELECT 1
              FROM dw_prestage.territories_update
              WHERE a.territory_id = dw_prestage.territories_update.territory_id
              AND   dw_prestage.territories_update.ch_type = 2);

/* dimension -> update records as part of SCD1 maintenance */ 
UPDATE dw.territories
   SET TERRITORY = NVL(dw_prestage.territories.TERRITORY,'NA_GDW'),
       SUBSIDIARY = NVL(dw_prestage.territories.SUBSIDIARY,'NA_GDW'),
       SUBSIDIARY_ID = NVL(dw_prestage.territories.SUBSIDIARY_ID,-99),
       IS_INACTIVE = NVL(dw_prestage.territories.IS_INACTIVE,'NA_GDW')
FROM dw_prestage.territories
WHERE dw.territories.territory_id = dw_prestage.territories.territory_id
AND   EXISTS (SELECT 1
              FROM dw_prestage.territories_update
              WHERE dw_prestage.territories.territory_id = dw_prestage.territories_update.territory_id
              AND   dw_prestage.territories_update.ch_type = 1);

/* dimension -> logically delete dw records */ 
UPDATE dw.territories
   SET DATE_ACTIVE_TO = sysdate -1,
       dw_active = 'I'
FROM dw_prestage.territories_delete
WHERE dw.territories.territory_id = dw_prestage.territories_delete.territory_id;

COMMIT;