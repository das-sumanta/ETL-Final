/* prestage - drop intermediate insert table */
DROP TABLE if exists dw_prestage.locations_insert;

/* prestage - create intermediate insert table*/
CREATE TABLE dw_prestage.locations_insert 
AS
SELECT *
FROM dw_prestage.locations
WHERE EXISTS (SELECT 1
              FROM (SELECT location_id
                    FROM (SELECT location_id
                          FROM dw_prestage.locations
                          MINUS
                          SELECT location_id
                          FROM dw_stage.locations)) a
              WHERE dw_prestage.locations.location_id = a.location_id);
/* prestage - drop intermediate update table*/
DROP TABLE if exists dw_prestage.locations_update;

/* prestage - create intermediate update table*/
CREATE TABLE dw_prestage.locations_update 
AS
SELECT decode(SUM(ch_type),
             3,2,
             SUM(ch_type)
       )  ch_type,
       location_id
FROM (SELECT location_id,
             CH_TYPE
      FROM (
      
      
           SELECT location_id,PARENT_ID,LINE_OF_BUSINESS_ID,'2' CH_TYPE 
           FROM dw_prestage.locations
      MINUS
      SELECT location_id,
             PARENT_ID,
             LINE_OF_BUSINESS_ID,
             '2' CH_TYPE
      FROM dw_stage.locations)
      UNION ALL
      SELECT location_id,
             CH_TYPE
      FROM (
      
      
           SELECT location_id,ADDRESS,ADDRESS_ONE,ADDRESS_TWO,ADDRESS_THREE,CITY,STATE,COUNTRY,ZIPCODE,ISINACTIVE,ATTENTION,BRANCH_ID,FULL_NAME,INVENTORY_AVAILABLE,INVENTORY_AVAILABLE_WEB_STORE,IS_INCLUDE_IN_SUPPLY_PLANNING,RETURN_ADDRESS_ONE,RETURN_ADDRESS_TWO,RETURN_CITY,RETURN_STATE,RETURN_COUNTRY,RETURN_ZIPCODE,'1' CH_TYPE 
           FROM dw_prestage.locations
      MINUS
      SELECT location_id,
             ADDRESS,
             ADDRESS_ONE,
             ADDRESS_TWO,
             ADDRESS_THREE,
             CITY,
             STATE,
             COUNTRY,
             ZIPCODE,
             ISINACTIVE,
             ATTENTION,
             BRANCH_ID,
             FULL_NAME,
             INVENTORY_AVAILABLE,
             INVENTORY_AVAILABLE_WEB_STORE,
             IS_INCLUDE_IN_SUPPLY_PLANNING,
             RETURN_ADDRESS_ONE,
             RETURN_ADDRESS_TWO,
             RETURN_CITY,
             RETURN_STATE,
             RETURN_COUNTRY,
             RETURN_ZIPCODE,
             '1' CH_TYPE
      FROM dw_stage.locations)) a
WHERE NOT EXISTS (SELECT 1
                  FROM dw_prestage.locations_insert
                  WHERE dw_prestage.locations_insert.location_id = a.location_id)
GROUP BY location_id;

/* prestage - drop intermediate delete table*/
DROP TABLE if exists dw_prestage.locations_delete;

/* prestage - create intermediate delete table*/
CREATE TABLE dw_prestage.locations_delete 
AS
SELECT *
FROM dw_stage.locations
WHERE EXISTS (SELECT 1
              FROM (SELECT location_id
                    FROM (SELECT location_id
                          FROM dw_stage.locations
                          MINUS
                          SELECT location_id
                          FROM dw_prestage.locations)) a
              WHERE dw_stage.locations.location_id = a.location_id);

 /* prestage-> stage*/             
SELECT 'no of prestage vendor records identified to inserted -->' ||count(1)
FROM dw_prestage.locations_insert;

/* prestage-> stage*/
SELECT 'no of prestage vendor records identified to updated -->' ||count(1)
FROM dw_prestage.locations_update;

/* prestage-> stage*/
SELECT 'no of prestage vendor records identified to deleted -->' ||count(1)
FROM dw_prestage.locations_delete;

/* stage->delete from stage records to be updated */

DELETE
FROM dw_stage.locations USING dw_prestage.locations_update
WHERE dw_stage.locations.location_id = dw_prestage.locations_update.location_id;

/* stage->delete from stage records which have been deleted */ 
DELETE
FROM dw_stage.locations USING dw_prestage.locations_delete
WHERE dw_stage.locations.location_id = dw_prestage.locations_delete.location_id;

/* stage->insert into stage records which have been created */ 
INSERT INTO dw_stage.locations (ADDRESS,
       ADDRESSEE,
       ADDRESS_ONE,
       ADDRESS_THREE,
       ADDRESS_TWO,
       ATTENTION,
       BRANCH_ID,
       CITY,
       COUNTRY,
       DATE_LAST_MODIFIED,
       FULL_NAME,
       INVENTORY_AVAILABLE,
       INVENTORY_AVAILABLE_WEB_STORE,
       ISINACTIVE,
       IS_INCLUDE_IN_SUPPLY_PLANNING,
       LINE_OF_BUSINESS_ID,
       LOCATION_EXTID,
       LOCATION_ID,
       NAME,
       PARENT_ID,
       PHONE,
       RETURNADDRESS,
       RETURN_ADDRESS_ONE,
       RETURN_ADDRESS_TWO,
       RETURN_CITY,
       RETURN_COUNTRY,
       RETURN_STATE,
       RETURN_ZIPCODE,
       STATE,
       TRAN_NUM_PREFIX,
       ZIPCODE)
SELECT ADDRESS,
       ADDRESSEE,
       ADDRESS_ONE,
       ADDRESS_THREE,
       ADDRESS_TWO,
       ATTENTION,
       BRANCH_ID,
       CITY,
       COUNTRY,
       DATE_LAST_MODIFIED,
       FULL_NAME,
       INVENTORY_AVAILABLE,
       INVENTORY_AVAILABLE_WEB_STORE,
       ISINACTIVE,
       IS_INCLUDE_IN_SUPPLY_PLANNING,
       LINE_OF_BUSINESS_ID,
       LOCATION_EXTID,
       LOCATION_ID,
       NAME,
       PARENT_ID,
       PHONE,
       RETURNADDRESS,
       RETURN_ADDRESS_ONE,
       RETURN_ADDRESS_TWO,
       RETURN_CITY,
       RETURN_COUNTRY,
       RETURN_STATE,
       RETURN_ZIPCODE,
       STATE,
       TRAN_NUM_PREFIX,
       ZIPCODE
FROM dw_prestage.locations_insert;

/* stage->insert into stage records which have been updated */ 
INSERT INTO dw_stage.locations (ADDRESS,
       ADDRESSEE,
       ADDRESS_ONE,
       ADDRESS_THREE,
       ADDRESS_TWO,
       ATTENTION,
       BRANCH_ID,
       CITY,
       COUNTRY,
       DATE_LAST_MODIFIED,
       FULL_NAME,
       INVENTORY_AVAILABLE,
       INVENTORY_AVAILABLE_WEB_STORE,
       ISINACTIVE,
       IS_INCLUDE_IN_SUPPLY_PLANNING,
       LINE_OF_BUSINESS_ID,
       LOCATION_EXTID,
       LOCATION_ID,
       NAME,
       PARENT_ID,
       PHONE,
       RETURNADDRESS,
       RETURN_ADDRESS_ONE,
       RETURN_ADDRESS_TWO,
       RETURN_CITY,
       RETURN_COUNTRY,
       RETURN_STATE,
       RETURN_ZIPCODE,
       STATE,
       TRAN_NUM_PREFIX,
       ZIPCODE)
SELECT ADDRESS,
       ADDRESSEE,
       ADDRESS_ONE,
       ADDRESS_THREE,
       ADDRESS_TWO,
       ATTENTION,
       BRANCH_ID,
       CITY,
       COUNTRY,
       DATE_LAST_MODIFIED,
       FULL_NAME,
       INVENTORY_AVAILABLE,
       INVENTORY_AVAILABLE_WEB_STORE,
       ISINACTIVE,
       IS_INCLUDE_IN_SUPPLY_PLANNING,
       LINE_OF_BUSINESS_ID,
       LOCATION_EXTID,
       LOCATION_ID,
       NAME,
       PARENT_ID,
       PHONE,
       RETURNADDRESS,
       RETURN_ADDRESS_ONE,
       RETURN_ADDRESS_TWO,
       RETURN_CITY,
       RETURN_COUNTRY,
       RETURN_STATE,
       RETURN_ZIPCODE,
       STATE,
       TRAN_NUM_PREFIX,
       ZIPCODE
FROM dw_prestage.locations
WHERE EXISTS (SELECT 1
              FROM dw_prestage.locations_update
              WHERE dw_prestage.locations_update.location_id = dw_prestage.locations.location_id);

COMMIT;

/* dimention->insert new records in dim locations */ /*==========================assumed that dimensions will be full extraction and hence dw_prestage.locations will have all the records  =====*/ /*==========================for the first run. =====================================================================================================================================*/ 
INSERT INTO dw.locations
(
  LOCATION_ID,
  NAME,
  PARENT_ID,
  PARENT_NAME,
  ADDRESS,
  ADDRESS_ONE,
  ADDRESS_TWO,
  ADDRESS_THREE,
  CITY,
  STATE,
  COUNTRY,
  ZIPCODE,
  ISINACTIVE,
  ATTENTION,
  BRANCH_ID,
  FULL_NAME,
  INVENTORY_AVAILABLE,
  INVENTORY_AVAILABLE_WEB_STORE,
  IS_INCLUDE_IN_SUPPLY_PLANNING,
  LINE_OF_BUSINESS,
  LINE_OF_BUSINESS_ID,
  RETURN_ADDRESS_ONE,
  RETURN_ADDRESS_TWO,
  RETURN_CITY,
  RETURN_STATE,
  RETURN_COUNTRY,
  RETURN_ZIPCODE,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE
)
SELECT A.location_id AS location_id,
       DECODE(LENGTH(A.NAME),
             0,'NA_GDW',
             A.NAME
       ) AS NAME,
       NVL(A.PARENT_ID,-99) AS PARENT_ID,
       NVL(B.NAME,'NA_GDW') AS PARENT_NAME,
       DECODE(LENGTH(A.ADDRESS),
             0,'NA_GDW',
             A.ADDRESS
       ) AS ADDRESS,
       DECODE(LENGTH(A.ADDRESS_ONE),
             0,'NA_GDW',
             A.ADDRESS_ONE
       ) AS ADDRESS_ONE,
       DECODE(LENGTH(A.ADDRESS_TWO),
             0,'NA_GDW',
             A.ADDRESS_TWO
       ) AS ADDRESS_TWO,
       DECODE(LENGTH(A.ADDRESS_THREE),
             0,'NA_GDW',
             A.ADDRESS_THREE
       ) AS ADDRESS_THREE,
       DECODE(LENGTH(A.CITY),
             0,'NA_GDW',
             A.CITY
       ) AS CITY,
       DECODE(LENGTH(A.STATE),
             0,'NA_GDW',
             A.STATE
       ) AS STATE,
       DECODE(LENGTH(A.COUNTRY),
             0,'NA_GDW',
             A.COUNTRY
       ) AS COUNTRY,
       DECODE(LENGTH(A.ZIPCODE),
             0,'NA_GDW',
             A.ZIPCODE
       ) AS ZIPCODE,
       DECODE(LENGTH(A.ISINACTIVE),
             0,'NA_GDW',
             A.ISINACTIVE
       ) AS ISINACTIVE,
       DECODE(LENGTH(A.ATTENTION),
             0,'NA_GDW',
             A.ATTENTION
       ) AS ATTENTION,
       DECODE(LENGTH(A.BRANCH_ID),
             0,'NA_GDW',
             A.BRANCH_ID
       ) AS BRANCH_ID,
       DECODE(LENGTH(A.FULL_NAME),
             0,'NA_GDW',
             A.FULL_NAME
       ) AS FULL_NAME,
       DECODE(LENGTH(A.INVENTORY_AVAILABLE),
             0,'NA_GDW',
             A.INVENTORY_AVAILABLE
       ) AS INVENTORY_AVAILABLE,
       DECODE(LENGTH(A.INVENTORY_AVAILABLE_WEB_STORE),
             0,'NA_GDW',
             A.INVENTORY_AVAILABLE_WEB_STORE
       ) AS INVENTORY_AVAILABLE_WEB_STORE,
       DECODE(LENGTH(A.IS_INCLUDE_IN_SUPPLY_PLANNING),
             0,'NA_GDW',
             A.IS_INCLUDE_IN_SUPPLY_PLANNING
       ) AS IS_INCLUDE_IN_SUPPLY_PLANNING,
       NVL(C.NAME,'NA_GDW') AS LINE_OF_BUSINESS,
       NVL(A.LINE_OF_BUSINESS_ID -99) AS LINE_OF_BUSINESS_ID,
       DECODE(LENGTH(A.RETURN_ADDRESS_ONE),
             0,'NA_GDW',
             A.RETURN_ADDRESS_ONE
       ) AS RETURN_ADDRESS_ONE,
       DECODE(LENGTH(A.RETURN_ADDRESS_TWO),
             0,'NA_GDW',
             A.RETURN_ADDRESS_TWO
       ) AS RETURN_ADDRESS_TWO,
       DECODE(LENGTH(A.RETURN_CITY),
             0,'NA_GDW',
             A.RETURN_CITY
       ) AS RETURN_CITY,
       DECODE(LENGTH(A.RETURN_STATE),
             0,'NA_GDW',
             A.RETURN_STATE
       ) AS RETURN_STATE,
       DECODE(LENGTH(A.RETURN_COUNTRY),
             0,'NA_GDW',
             A.RETURN_COUNTRY
       ) AS RETURN_COUNTRY,
       DECODE(LENGTH(A.RETURN_ZIPCODE),
             0,'NA_GDW',
             A.RETURN_ZIPCODE
       ) AS RETURN_ZIPCODE,
       sysdate AS DATE_ACTIVE_FROM,
       '9999-12-31 23:59:59' AS DATE_ACTIVE_TO,
       'A' AS DW_ACTIVE
FROM dw_prestage.locations_insert A,
     dw_prestage.locations B,
     dw.classes C
WHERE A.PARENT_ID = B.location_id (+)
AND   A.line_of_business_id = C.class_id (+);

/*==============================================assumed since this is an update the record/s already exists in dim table===========================================================*/ /*===============================================only one record will be there with dw_active column as 'A'========================================================================*/ /* update old record as part of SCD2 maintenance*/ 
UPDATE dw.locations
   SET dw_active = 'I',
       DATE_ACTIVE_TO = (sysdate -1)
WHERE dw_active = 'A'
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1
              FROM dw_prestage.locations_update
              WHERE dw.locations.location_id = dw_prestage.locations_update.location_id
              AND   dw_prestage.locations_update.ch_type = 2);

/* dimention->insert the new records as part of SCD2 maintenance*/ 
INSERT INTO dw.locations
(
  LOCATION_ID,
  NAME,
  PARENT_ID,
  PARENT_NAME,
  ADDRESS,
  ADDRESS_ONE,
  ADDRESS_TWO,
  ADDRESS_THREE,
  CITY,
  STATE,
  COUNTRY,
  ZIPCODE,
  ISINACTIVE,
  ATTENTION,
  BRANCH_ID,
  FULL_NAME,
  INVENTORY_AVAILABLE,
  INVENTORY_AVAILABLE_WEB_STORE,
  IS_INCLUDE_IN_SUPPLY_PLANNING,
  LINE_OF_BUSINESS,
  LINE_OF_BUSINESS_ID,
  RETURN_ADDRESS_ONE,
  RETURN_ADDRESS_TWO,
  RETURN_CITY,
  RETURN_STATE,
  RETURN_COUNTRY,
  RETURN_ZIPCODE,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE
)
SELECT A.location_id AS location_id,
       DECODE(LENGTH(A.NAME),
             0,'NA_GDW',
             A.NAME
       ) AS NAME,
       NVL(A.PARENT_ID,-99) AS PARENT_ID,
       NVL(B.NAME,'NA_GDW') AS PARENT_NAME,
       DECODE(LENGTH(A.ADDRESS),
             0,'NA_GDW',
             A.ADDRESS
       ) AS ADDRESS,
       DECODE(LENGTH(A.ADDRESS_ONE),
             0,'NA_GDW',
             A.ADDRESS_ONE
       ) AS ADDRESS_ONE,
       DECODE(LENGTH(A.ADDRESS_TWO),
             0,'NA_GDW',
             A.ADDRESS_TWO
       ) AS ADDRESS_TWO,
       DECODE(LENGTH(A.ADDRESS_THREE),
             0,'NA_GDW',
             A.ADDRESS_THREE
       ) AS ADDRESS_THREE,
       DECODE(LENGTH(A.CITY),
             0,'NA_GDW',
             A.CITY
       ) AS CITY,
       DECODE(LENGTH(A.STATE),
             0,'NA_GDW',
             A.STATE
       ) AS STATE,
       DECODE(LENGTH(A.COUNTRY),
             0,'NA_GDW',
             A.COUNTRY
       ) AS COUNTRY,
       DECODE(LENGTH(A.ZIPCODE),
             0,'NA_GDW',
             A.ZIPCODE
       ) AS ZIPCODE,
       DECODE(LENGTH(A.ISINACTIVE),
             0,'NA_GDW',
             A.ISINACTIVE
       ) AS ISINACTIVE,
       DECODE(LENGTH(A.ATTENTION),
             0,'NA_GDW',
             A.ATTENTION
       ) AS ATTENTION,
       DECODE(LENGTH(A.BRANCH_ID),
             0,'NA_GDW',
             A.BRANCH_ID
       ) AS BRANCH_ID,
       DECODE(LENGTH(A.FULL_NAME),
             0,'NA_GDW',
             A.FULL_NAME
       ) AS FULL_NAME,
       DECODE(LENGTH(A.INVENTORY_AVAILABLE),
             0,'NA_GDW',
             A.INVENTORY_AVAILABLE
       ) AS INVENTORY_AVAILABLE,
       DECODE(LENGTH(A.INVENTORY_AVAILABLE_WEB_STORE),
             0,'NA_GDW',
             A.INVENTORY_AVAILABLE_WEB_STORE
       ) AS INVENTORY_AVAILABLE_WEB_STORE,
       DECODE(LENGTH(A.IS_INCLUDE_IN_SUPPLY_PLANNING),
             0,'NA_GDW',
             A.IS_INCLUDE_IN_SUPPLY_PLANNING
       ) AS IS_INCLUDE_IN_SUPPLY_PLANNING,
       NVL(C.NAME,'NA_GDW') AS LINE_OF_BUSINESS,
       NVL(A.LINE_OF_BUSINESS_ID -99) AS LINE_OF_BUSINESS_ID,
       DECODE(LENGTH(A.RETURN_ADDRESS_ONE),
             0,'NA_GDW',
             A.RETURN_ADDRESS_ONE
       ) AS RETURN_ADDRESS_ONE,
       DECODE(LENGTH(A.RETURN_ADDRESS_TWO),
             0,'NA_GDW',
             A.RETURN_ADDRESS_TWO
       ) AS RETURN_ADDRESS_TWO,
       DECODE(LENGTH(A.RETURN_CITY),
             0,'NA_GDW',
             A.RETURN_CITY
       ) AS RETURN_CITY,
       DECODE(LENGTH(A.RETURN_STATE),
             0,'NA_GDW',
             A.RETURN_STATE
       ) AS RETURN_STATE,
       DECODE(LENGTH(A.RETURN_COUNTRY),
             0,'NA_GDW',
             A.RETURN_COUNTRY
       ) AS RETURN_COUNTRY,
       DECODE(LENGTH(A.RETURN_ZIPCODE),
             0,'NA_GDW',
             A.RETURN_ZIPCODE
       ) AS RETURN_ZIPCODE,
       sysdate AS DATE_ACTIVE_FROM,
       '9999-12-31 23:59:59' AS DATE_ACTIVE_TO,
       'A' AS DW_ACTIVE
FROM dw_prestage.locations A,
     dw_prestage.locations B,
     dw.classes C
WHERE A.PARENT_ID = B.location_id (+)
AND   A.line_of_business_id = C.class_id (+)
AND   EXISTS (SELECT 1
              FROM dw_prestage.locations_update
              WHERE a.location_id = dw_prestage.locations_update.location_id
              AND   dw_prestage.locations_update.ch_type = 2);

/* dimention->update SCD1 */ 
UPDATE dw.locations
   SET ADDRESS = DECODE(LENGTH(dw_prestage.locations.ADDRESS),0,'NA_GDW',dw_prestage.locations.ADDRESS),
       ADDRESS_ONE = DECODE(LENGTH(dw_prestage.locations.ADDRESS_ONE),0,'NA_GDW',dw_prestage.locations.ADDRESS_ONE),
       ADDRESS_TWO = DECODE(LENGTH(dw_prestage.locations.ADDRESS_TWO),0,'NA_GDW',dw_prestage.locations.ADDRESS_TWO),
       ADDRESS_THREE = DECODE(LENGTH(dw_prestage.locations.ADDRESS_THREE),0,'NA_GDW',dw_prestage.locations.ADDRESS_THREE),
       CITY = DECODE(LENGTH(dw_prestage.locations.CITY),0,'NA_GDW',dw_prestage.locations.CITY),
       STATE = DECODE(LENGTH(dw_prestage.locations.STATE),0,'NA_GDW',dw_prestage.locations.STATE),
       COUNTRY = DECODE(LENGTH(dw_prestage.locations.COUNTRY),0,'NA_GDW',dw_prestage.locations.COUNTRY),
       ZIPCODE = DECODE(LENGTH(dw_prestage.locations.ZIPCODE),0,'NA_GDW',dw_prestage.locations.ZIPCODE),
       ISINACTIVE = DECODE(LENGTH(dw_prestage.locations.ISINACTIVE),0,'NA_GDW',dw_prestage.locations.ISINACTIVE),
       ATTENTION = DECODE(LENGTH(dw_prestage.locations.ATTENTION),0,'NA_GDW',dw_prestage.locations.ATTENTION),
       BRANCH_ID = DECODE(LENGTH(dw_prestage.locations.BRANCH_ID),0,'NA_GDW',dw_prestage.locations.BRANCH_ID),
       FULL_NAME = DECODE(LENGTH(dw_prestage.locations.FULL_NAME),0,'NA_GDW',dw_prestage.locations.FULL_NAME),
       INVENTORY_AVAILABLE = DECODE(LENGTH(dw_prestage.locations.INVENTORY_AVAILABLE),0,'NA_GDW',dw_prestage.locations.INVENTORY_AVAILABLE),
       INVENTORY_AVAILABLE_WEB_STORE = DECODE(LENGTH(dw_prestage.locations.INVENTORY_AVAILABLE_WEB_STORE),0,'NA_GDW',dw_prestage.locations.INVENTORY_AVAILABLE_WEB_STORE),
       IS_INCLUDE_IN_SUPPLY_PLANNING = DECODE(LENGTH(dw_prestage.locations.IS_INCLUDE_IN_SUPPLY_PLANNING),0,'NA_GDW',dw_prestage.locations.IS_INCLUDE_IN_SUPPLY_PLANNING),
       RETURN_ADDRESS_ONE = DECODE(LENGTH(dw_prestage.locations.RETURN_ADDRESS_ONE),0,'NA_GDW',dw_prestage.locations.RETURN_ADDRESS_ONE),
       RETURN_ADDRESS_TWO = DECODE(LENGTH(dw_prestage.locations.RETURN_ADDRESS_TWO),0,'NA_GDW',dw_prestage.locations.RETURN_ADDRESS_TWO),
       RETURN_CITY = DECODE(LENGTH(dw_prestage.locations.RETURN_CITY),0,'NA_GDW',dw_prestage.locations.RETURN_CITY),
       RETURN_STATE = DECODE(LENGTH(dw_prestage.locations.RETURN_STATE),0,'NA_GDW',dw_prestage.locations.RETURN_STATE),
       RETURN_COUNTRY = DECODE(LENGTH(dw_prestage.locations.RETURN_COUNTRY),0,'NA_GDW',dw_prestage.locations.RETURN_COUNTRY),
       RETURN_ZIPCODE = DECODE(LENGTH(dw_prestage.locations.RETURN_ZIPCODE),0,'NA_GDW',dw_prestage.locations.RETURN_ZIPCODE)
FROM dw_prestage.locations
WHERE dw.locations.location_id = dw_prestage.locations.location_id
AND   EXISTS (SELECT 1
              FROM dw_prestage.locations_update
              WHERE dw_prestage.locations.location_id = dw_prestage.locations_update.location_id
              AND   dw_prestage.locations_update.ch_type = 1);

/* dimention->logically delete dw records */ 
UPDATE dw.locations
   SET DATE_ACTIVE_TO = sysdate -1,
       dw_active = 'I'
FROM dw_prestage.locations_delete
WHERE dw.locations.location_id = dw_prestage.locations_delete.location_id;

COMMIT;


