/* prestage - drop intermediate insert table */
DROP TABLE if exists dw_prestage.vra_fact_insert;

/* prestage - create intermediate insert table*/
CREATE TABLE dw_prestage.vra_fact_insert 
AS
SELECT *
FROM dw_prestage.vra_fact
WHERE EXISTS (SELECT 1
FROM (SELECT TRANSACTION_ID, transaction_line_id FROM (SELECT TRANSACTION_ID, transaction_line_id FROM dw_prestage.vra_fact MINUS SELECT TRANSACTION_ID, transaction_line_id FROM dw_stage.vra_fact)) a
WHERE dw_prestage.vra_fact.TRANSACTION_ID = a.TRANSACTION_ID
AND   dw_prestage.vra_fact.transaction_line_id = a.transaction_line_id
);

/* prestage - drop intermediate update table*/
DROP TABLE if exists dw_prestage.vra_fact_update;

/* prestage - create intermediate update table*/
CREATE TABLE dw_prestage.vra_fact_update 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM (SELECT 
             TRANSACTION_ID         
            ,TRANSACTION_LINE_ID    
            ,VRA_NUMBER             
            ,VENDOR_ID              
            ,CREATED_FROM_ID        
            ,REF_TRX_NUMBER         
            ,CREATED_BY_ID          
            ,SHIPMENT_RECEIVED_DATE 
            ,CREATE_DATE            
            ,SUBSIDIARY_ID          
            ,DEPARTMENT_ID          
            ,ITEM_ID                
            ,LOCATION_ID            
            ,EXCHANGE_RATE          
            ,VENDOR_ADDRESS_LINE_1  
            ,VENDOR_ADDRESS_LINE_2	 
            ,VENDOR_ADDRESS_LINE_3	 
            ,VENDOR_CITY	           
            ,VENDOR_COUNTRY	       
            ,VENDOR_STATE	         
            ,VENDOR_ZIP	           
            ,VRA_STATUS             
            ,APPROVAL_STATUS        
            ,ITEM_COUNT             
            ,ITEM_GROSS_AMOUNT      
            ,AMOUNT                 
            ,AMOUNT_FOREIGN         
            ,NET_AMOUNT             
            ,NET_AMOUNT_FOREIGN     
            ,GROSS_AMOUNT           
            ,ITEM_UNIT_PRICE        
            ,QUANTITY_BILLED        
            ,QUANTITY_SHIPPED       
            ,CLOSE_DATE             
            ,TAX_ITEM_ID                  
            ,TAX_AMOUNT             
            ,TAX_AMOUNT_FOREIGN     
            ,VRA_TYPE               
            ,CURRENCY_ID            
            ,CLASS_ID               
            FROM dw_prestage.vra_fact
            MINUS
            SELECT 
             TRANSACTION_ID         
            ,TRANSACTION_LINE_ID    
            ,VRA_NUMBER             
            ,VENDOR_ID              
            ,CREATED_FROM_ID        
            ,REF_TRX_NUMBER         
            ,CREATED_BY_ID          
            ,SHIPMENT_RECEIVED_DATE 
            ,CREATE_DATE            
            ,SUBSIDIARY_ID          
            ,DEPARTMENT_ID          
            ,ITEM_ID                
            ,LOCATION_ID            
            ,EXCHANGE_RATE          
            ,VENDOR_ADDRESS_LINE_1  
            ,VENDOR_ADDRESS_LINE_2	 
            ,VENDOR_ADDRESS_LINE_3	 
            ,VENDOR_CITY	           
            ,VENDOR_COUNTRY	       
            ,VENDOR_STATE	         
            ,VENDOR_ZIP	           
            ,VRA_STATUS             
            ,APPROVAL_STATUS        
            ,ITEM_COUNT             
            ,ITEM_GROSS_AMOUNT      
            ,AMOUNT                 
            ,AMOUNT_FOREIGN         
            ,NET_AMOUNT             
            ,NET_AMOUNT_FOREIGN     
            ,GROSS_AMOUNT           
            ,ITEM_UNIT_PRICE        
            ,QUANTITY_BILLED        
            ,QUANTITY_SHIPPED       
            ,CLOSE_DATE             
            ,TAX_ITEM_ID                  
            ,TAX_AMOUNT             
            ,TAX_AMOUNT_FOREIGN     
            ,VRA_TYPE               
            ,CURRENCY_ID            
            ,CLASS_ID 
            FROM dw_stage.vra_fact)) a
WHERE NOT EXISTS (SELECT 1 FROM dw_prestage.vra_fact_insert WHERE dw_prestage.vra_fact_insert.TRANSACTION_ID = a.TRANSACTION_ID AND   dw_prestage.vra_fact_insert.transaction_line_id = a.transaction_line_id);

/* prestage - drop intermediate no change track table*/
DROP TABLE if exists dw_prestage.vra_fact_nochange;

/* prestage - create intermediate no change track table*/
CREATE TABLE dw_prestage.vra_fact_nochange 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vra_fact
      MINUS
      (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vra_fact_insert
      UNION ALL
      SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vra_fact_update));

/* prestage-> stage*/
SELECT 'no of vra fact records ingested in staging -->' ||count(1)
FROM dw_prestage.vra_fact;

/* prestage-> stage*/
SELECT 'no of vra fact records identified to inserted -->' ||count(1)
FROM dw_prestage.vra_fact_insert;

/* prestage-> stage*/
SELECT 'no of vra fact records identified to updated -->' ||count(1)
FROM dw_prestage.vra_fact_update;

/* prestage-> stage*/
SELECT 'no of vra fact records identified as no change -->' ||count(1)
FROM dw_prestage.vra_fact_nochange;

--D --A = B + C + D
/* stage -> delete from stage records to be updated */ 
DELETE
FROM dw_stage.vra_fact USING dw_prestage.vra_fact_update
WHERE dw_stage.vra_fact.transaction_id = dw_prestage.vra_fact_update.transaction_id
AND   dw_stage.vra_fact.transaction_line_id = dw_prestage.vra_fact_update.transaction_line_id;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.vra_fact
SELECT *
FROM dw_prestage.vra_fact_insert;

/* stage -> insert into stage records which have been updated */ 
INSERT INTO dw_stage.vra_fact
SELECT *
FROM dw_prestage.vra_fact
WHERE EXISTS (SELECT 1
              FROM dw_prestage.vra_fact_update
              WHERE dw_prestage.vra_fact_update.transaction_id = dw_prestage.vra_fact.transaction_id
              AND   dw_prestage.vra_fact_update.transaction_line_id = dw_prestage.vra_fact.transaction_line_id);

COMMIT;

/* fact -> INSERT NEW RECORDS WHICH HAVE ALL VALID DIMENSIONS */ 
INSERT INTO dw.vra_fact
(   VRA_NUMBER            
  ,TRANSACTION_ID        
  ,TRANSACTION_LINE_ID   
  ,VENDOR_KEY            
  ,REF_TRX_NUMBER 
  ,REQUESTER_KEY       
  ,RECEIVE_DATE_KEY      
  ,CREATE_DATE_KEY       
  ,SUBSIDIARY_KEY        
  ,LOCATION_KEY          
  ,COST_CENTER_KEY       
  ,EXCHANGE_RATE         
  ,ITEM_KEY              
  ,VENDOR_ADDRESS_LINE_1 
  ,VENDOR_ADDRESS_LINE_2 
  ,VENDOR_ADDRESS_LINE_3 
  ,VENDOR_CITY           
  ,VENDOR_COUNTRY        
  ,VENDOR_STATE          
  ,VENDOR_ZIP            
  ,QUANTITY              
  ,RATE                  
  ,AMOUNT                
  ,QUANTITY_BILLED       
  ,QUANTITY_SHIPPED      
  ,ITEM_GROSS_AMOUNT     
  ,AMOUNT_FOREIGN        
  ,NET_AMOUNT            
  ,NET_AMOUNT_FOREIGN    
  ,GROSS_AMOUNT           
  ,TAX_ITEM_KEY          
  ,TAX_AMOUNT_FOREIGN    
  ,TAX_AMOUNT
  ,CURRENCY_KEY          
  ,CLASS_KEY             
  ,CLOSE_DATE_KEY        
  ,VRA_TYPE              
  ,STATUS                
  ,APPROVAL_STATUS       
  ,CREATION_DATE         
  ,LAST_MODIFIED_DATE    
  ,DATE_ACTIVE_FROM      
  ,DATE_ACTIVE_TO        
  ,DW_CURRENT                        
)
SELECT NVL(A.vra_number,'NA_GDW') AS vra_number,
       A.transaction_id AS transaction_id,
       A.transaction_line_id AS transaction_line_id,
       B.VENDOR_KEY AS VENDOR_KEY,
       NVL(A.REF_TRX_NUMBER,'NA_GDW') AS REF_TRX_NUMBER,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       H.DATE_KEY AS RECEIVE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       M.ITEM_KEY AS ITEM_KEY,
       NVL(A.VENDOR_ADDRESS_LINE_1,'NA_GDW') AS VENDOR_ADDRESS_LINE_1,
       NVL(A.VENDOR_ADDRESS_LINE_2,'NA_GDW') AS VENDOR_ADDRESS_LINE_2,
       NVL(A.VENDOR_ADDRESS_LINE_3,'NA_GDW') AS VENDOR_ADDRESS_LINE_3,
       NVL(A.VENDOR_CITY,'NA_GDW') AS VENDOR_CITY,
       NVL(A.VENDOR_COUNTRY,'NA_GDW') AS VENDOR_COUNTRY,
       NVL(A.VENDOR_STATE,'NA_GDW') AS VENDOR_STATE,
       NVL(A.VENDOR_ZIP,'NA_GDW') AS VENDOR_ZIP,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.QUANTITY_BILLED AS QUANTITY_BILLED,
       QUANTITY_SHIPPED AS QUANTITY_SHIPPED,
       A.ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN,
       A.NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT_FOREIGN,
       A.TAX_AMOUNT,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       N.DATE_KEY AS CLOSE_DATE_KEY,
       NVL(A.VRA_TYPE,'NA_GDW') AS VRA_TYPE,
       NVL(A.VRA_STATUS,'NA_GDW') AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.vra_fact_insert A
  INNER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)
  INNER JOIN DW.EMPLOYEES C ON (NVL (A.CREATED_BY_ID,-99) = C.EMPLOYEE_ID)
  INNER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  INNER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.SHIPMENT_RECEIVED_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  INNER JOIN DW.DWDATE N ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = N.DATE_ID)
  INNER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)
  INNER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW.COST_CENTER R ON (NVL (A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  INNER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW.CLASSES S ON (NVL (A.CLASS_ID,-99) = S.CLASS_ID)
WHERE A.LINE_TYPE = 'VRA_LINE'
ORDER BY A.vra_number;
  
/* fact -> INSERT NEW RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.vra_fact_error
(
  RUNID,
  VRA_NUMBER,
  TRANSACTION_ID,
  TRANSACTION_LINE_ID,
  VENDOR_KEY,
  REF_TRX_NUMBER,
  REQUESTER_KEY,
  RECEIVE_DATE_KEY,
  CREATE_DATE_KEY,
  SUBSIDIARY_KEY,
  LOCATION_KEY,
  COST_CENTER_KEY,
  EXCHANGE_RATE,
  ITEM_KEY,
  VENDOR_ADDRESS_LINE_1,
  VENDOR_ADDRESS_LINE_2,
  VENDOR_ADDRESS_LINE_3,
  VENDOR_CITY,
  VENDOR_COUNTRY,
  VENDOR_STATE,
  VENDOR_ZIP,
  QUANTITY,
  RATE,
  AMOUNT,
  QUANTITY_BILLED,
  QUANTITY_SHIPPED,
  ITEM_GROSS_AMOUNT,
  AMOUNT_FOREIGN,
  NET_AMOUNT,
  NET_AMOUNT_FOREIGN,
  GROSS_AMOUNT,
  TAX_ITEM_KEY,
  TAX_AMOUNT_FOREIGN,
  TAX_AMOUNT,
  CLOSE_DATE_KEY,
  VRA_TYPE,
  STATUS,
  APPROVAL_STATUS,
  CREATION_DATE,
  LAST_MODIFIED_DATE,
  CURRENCY_KEY,
  CLASS_KEY,
  VENDOR_ID,
  REQUESTER_ID,
  RECEIVE_DATE,
  SUBSIDIARY_ID,
  LOCATION_ID,
  COST_CENTER_ID,
  ITEM_ID,
  TAX_ITEM_ID,
  CLOSE_DATE,
  CURRENCY_ID,
  CLASS_ID,
  RECORD_STATUS,
  DW_CREATION_DATE
)
SELECT A.RUNID,
       NVL(A.vra_number,'NA_GDW') AS vra_number,
       A.transaction_id AS transaction_id,
       A.transaction_line_id AS transaction_line_id,
       B.VENDOR_KEY AS VENDOR_KEY,
       NVL(A.REF_TRX_NUMBER,'NA_GDW') AS REF_TRX_NUMBER,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       H.DATE_KEY AS RECEIVE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       M.ITEM_KEY AS ITEM_KEY,
       NVL(A.VENDOR_ADDRESS_LINE_1,'NA_GDW') AS VENDOR_ADDRESS_LINE_1,
       NVL(A.VENDOR_ADDRESS_LINE_2,'NA_GDW') AS VENDOR_ADDRESS_LINE_2,
       NVL(A.VENDOR_ADDRESS_LINE_3,'NA_GDW') AS VENDOR_ADDRESS_LINE_3,
       NVL(A.VENDOR_CITY,'NA_GDW') AS VENDOR_CITY,
       NVL(A.VENDOR_COUNTRY,'NA_GDW') AS VENDOR_COUNTRY,
       NVL(A.VENDOR_STATE,'NA_GDW') AS VENDOR_STATE,
       NVL(A.VENDOR_ZIP,'NA_GDW') AS VENDOR_ZIP,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.QUANTITY_BILLED AS QUANTITY_BILLED,
       QUANTITY_SHIPPED AS QUANTITY_SHIPPED,
       A.ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN,
       A.NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT_FOREIGN,
       A.TAX_AMOUNT,
       N.DATE_KEY AS CLOSE_DATE_KEY,
       NVL(A.VRA_TYPE,'NA_GDW') AS VRA_TYPE,
       NVL(A.VRA_STATUS,'NA_GDW') AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.VENDOR_ID,
       A.CREATED_BY_ID,
       A.SHIPMENT_RECEIVED_DATE,
       A.SUBSIDIARY_ID,
       A.LOCATION_ID,
       A.DEPARTMENT_ID,
       A.ITEM_ID,
       A.TAX_ITEM_ID,
       A.CLOSE_DATE,
       A.CURRENCY_ID,
       A.CLASS_ID,
       'ERROR' AS RECORD_STATUS,
       SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.vra_fact_insert A
  LEFT OUTER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)
  LEFT OUTER JOIN DW.EMPLOYEES C ON (NVL (A.CREATED_BY_ID,-99) = C.EMPLOYEE_ID)
  LEFT OUTER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  LEFT OUTER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.SHIPMENT_RECEIVED_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  LEFT OUTER JOIN DW.DWDATE N ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = N.DATE_ID)
  LEFT OUTER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)
  LEFT OUTER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  LEFT OUTER JOIN DW.COST_CENTER R ON (NVL (A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  LEFT OUTER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  LEFT OUTER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW.CLASSES S ON (NVL (A.CLASS_ID,-99) = S.CLASS_ID)
WHERE A.LINE_TYPE = 'VRA_LINE'
AND   (B.VENDOR_KEY IS NULL OR 
C.EMPLOYEE_KEY IS NULL OR 
F.DATE_KEY IS NULL OR 
H.DATE_KEY IS NULL OR 
N.DATE_KEY IS NULL OR 
G.SUBSIDIARY_KEY IS NULL OR 
L.LOCATION_KEY IS NULL OR 
R.DEPARTMENT_KEY IS NULL OR 
M.ITEM_KEY IS NULL OR 
Q.TAX_ITEM_KEY IS NULL OR 
K.CURRENCY_KEY IS NULL OR 
S.CLASS_KEY IS NULL);

/* fact -> UPDATE THE OLD RECORDS SETTING THE CURRENT FLAG VALUE TO 0 */  
UPDATE dw.vra_fact SET dw_current = 0,DATE_ACTIVE_TO = (sysdate -1) WHERE dw_current = 1
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1 FROM dw_prestage.vra_fact_update 
WHERE dw.vra_fact.transaction_id = dw_prestage.vra_fact_update.transaction_id 
AND   dw.vra_fact.transaction_line_id = dw_prestage.vra_fact_update.transaction_line_id);

/* fact -> INSERT THE FACT RECORDS WHICH HAVE BEEN UPDATED AT THE SOURCE */ 
INSERT INTO dw.vra_fact
(
  VRA_NUMBER,
  TRANSACTION_ID,
  TRANSACTION_LINE_ID,
  VENDOR_KEY,
  REF_TRX_NUMBER,
  REQUESTER_KEY,
  RECEIVE_DATE_KEY,
  CREATE_DATE_KEY,
  SUBSIDIARY_KEY,
  LOCATION_KEY,
  COST_CENTER_KEY,
  EXCHANGE_RATE,
  ITEM_KEY,
  VENDOR_ADDRESS_LINE_1,
  VENDOR_ADDRESS_LINE_2,
  VENDOR_ADDRESS_LINE_3,
  VENDOR_CITY,
  VENDOR_COUNTRY,
  VENDOR_STATE,
  VENDOR_ZIP,
  QUANTITY,
  RATE,
  AMOUNT,
  QUANTITY_BILLED,
  QUANTITY_SHIPPED,
  ITEM_GROSS_AMOUNT,
  AMOUNT_FOREIGN,
  NET_AMOUNT,
  NET_AMOUNT_FOREIGN,
  GROSS_AMOUNT,
  TAX_ITEM_KEY,
  TAX_AMOUNT_FOREIGN,
  TAX_AMOUNT,
  CURRENCY_KEY,
  CLASS_KEY,
  CLOSE_DATE_KEY,
  VRA_TYPE,
  STATUS,
  APPROVAL_STATUS,
  CREATION_DATE,
  LAST_MODIFIED_DATE,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_CURRENT
)
SELECT NVL(A.vra_number,'NA_GDW') AS vra_number,
       A.transaction_id AS transaction_id,
       A.transaction_line_id AS transaction_line_id,
       B.VENDOR_KEY AS VENDOR_KEY,
       NVL(A.REF_TRX_NUMBER,'NA_GDW') AS REF_TRX_NUMBER,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       H.DATE_KEY AS RECEIVE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       M.ITEM_KEY AS ITEM_KEY,
       NVL(A.VENDOR_ADDRESS_LINE_1,'NA_GDW') AS VENDOR_ADDRESS_LINE_1,
       NVL(A.VENDOR_ADDRESS_LINE_2,'NA_GDW') AS VENDOR_ADDRESS_LINE_2,
       NVL(A.VENDOR_ADDRESS_LINE_3,'NA_GDW') AS VENDOR_ADDRESS_LINE_3,
       NVL(A.VENDOR_CITY,'NA_GDW') AS VENDOR_CITY,
       NVL(A.VENDOR_COUNTRY,'NA_GDW') AS VENDOR_COUNTRY,
       NVL(A.VENDOR_STATE,'NA_GDW') AS VENDOR_STATE,
       NVL(A.VENDOR_ZIP,'NA_GDW') AS VENDOR_ZIP,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.QUANTITY_BILLED AS QUANTITY_BILLED,
       QUANTITY_SHIPPED AS QUANTITY_SHIPPED,
       A.ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN,
       A.NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT_FOREIGN,
       A.TAX_AMOUNT,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       N.DATE_KEY AS CLOSE_DATE_KEY,
       NVL(A.VRA_TYPE,'NA_GDW') AS VRA_TYPE,
       NVL(A.VRA_STATUS,'NA_GDW') AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.vra_fact A
  INNER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)
  INNER JOIN DW.EMPLOYEES C ON (NVL (A.CREATED_BY_ID,-99) = C.EMPLOYEE_ID)
  INNER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  INNER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.SHIPMENT_RECEIVED_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  INNER JOIN DW.DWDATE N ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = N.DATE_ID)
  INNER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)
  INNER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW.COST_CENTER R ON (NVL (A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  INNER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW.CLASSES S ON (NVL (A.CLASS_ID,-99) = S.CLASS_ID)
WHERE A.LINE_TYPE = 'VRA_LINE'
AND   EXISTS (SELECT 1
              FROM dw_prestage.vra_fact_update
              WHERE a.transaction_id = dw_prestage.vra_fact_update.transaction_id
              AND   a.transaction_line_id = dw_prestage.vra_fact_update.transaction_line_id);

/* fact -> INSERT UPDATED RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.vra_fact_error
(
  RUNID,
  VRA_NUMBER,
  TRANSACTION_ID,
  TRANSACTION_LINE_ID,
  VENDOR_KEY,
  REF_TRX_NUMBER,
  REQUESTER_KEY,
  RECEIVE_DATE_KEY,
  CREATE_DATE_KEY,
  SUBSIDIARY_KEY,
  LOCATION_KEY,
  COST_CENTER_KEY,
  EXCHANGE_RATE,
  ITEM_KEY,
  VENDOR_ADDRESS_LINE_1,
  VENDOR_ADDRESS_LINE_2,
  VENDOR_ADDRESS_LINE_3,
  VENDOR_CITY,
  VENDOR_COUNTRY,
  VENDOR_STATE,
  VENDOR_ZIP,
  QUANTITY,
  RATE,
  AMOUNT,
  QUANTITY_BILLED,
  QUANTITY_SHIPPED,
  ITEM_GROSS_AMOUNT,
  AMOUNT_FOREIGN,
  NET_AMOUNT,
  NET_AMOUNT_FOREIGN,
  GROSS_AMOUNT,
  TAX_ITEM_KEY,
  TAX_AMOUNT_FOREIGN,
  TAX_AMOUNT,
  CLOSE_DATE_KEY,
  VRA_TYPE,
  STATUS,
  APPROVAL_STATUS,
  CREATION_DATE,
  LAST_MODIFIED_DATE,
  CURRENCY_KEY,
  CLASS_KEY,
  VENDOR_ID,
  REQUESTER_ID,
  RECEIVE_DATE,
  SUBSIDIARY_ID,
  LOCATION_ID,
  COST_CENTER_ID,
  ITEM_ID,
  TAX_ITEM_ID,
  CLOSE_DATE,
  CURRENCY_ID,
  CLASS_ID,
  RECORD_STATUS,
  DW_CREATION_DATE
)
SELECT A.RUNID,
       NVL(A.vra_number,'NA_GDW') AS vra_number,
       A.transaction_id AS transaction_id,
       A.transaction_line_id AS transaction_line_id,
       B.VENDOR_KEY AS VENDOR_KEY,
       NVL(A.REF_TRX_NUMBER,'NA_GDW') AS REF_TRX_NUMBER,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       H.DATE_KEY AS RECEIVE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       M.ITEM_KEY AS ITEM_KEY,
       NVL(A.VENDOR_ADDRESS_LINE_1,'NA_GDW') AS VENDOR_ADDRESS_LINE_1,
       NVL(A.VENDOR_ADDRESS_LINE_2,'NA_GDW') AS VENDOR_ADDRESS_LINE_2,
       NVL(A.VENDOR_ADDRESS_LINE_3,'NA_GDW') AS VENDOR_ADDRESS_LINE_3,
       NVL(A.VENDOR_CITY,'NA_GDW') AS VENDOR_CITY,
       NVL(A.VENDOR_COUNTRY,'NA_GDW') AS VENDOR_COUNTRY,
       NVL(A.VENDOR_STATE,'NA_GDW') AS VENDOR_STATE,
       NVL(A.VENDOR_ZIP,'NA_GDW') AS VENDOR_ZIP,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.QUANTITY_BILLED AS QUANTITY_BILLED,
       QUANTITY_SHIPPED AS QUANTITY_SHIPPED,
       A.ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN,
       A.NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT_FOREIGN,
       A.TAX_AMOUNT,
       N.DATE_KEY AS CLOSE_DATE_KEY,
       NVL(A.VRA_TYPE,'NA_GDW') AS VRA_TYPE,
       NVL(A.VRA_STATUS,'NA_GDW') AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.VENDOR_ID,
       A.CREATED_BY_ID,
       A.SHIPMENT_RECEIVED_DATE,
       A.SUBSIDIARY_ID,
       A.LOCATION_ID,
       A.DEPARTMENT_ID,
       A.ITEM_ID,
       A.TAX_ITEM_ID,
       A.CLOSE_DATE,
       A.CURRENCY_ID,
       A.CLASS_ID,
       'ERROR' AS RECORD_STATUS,
       SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.vra_fact A
  LEFT OUTER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)
  LEFT OUTER JOIN DW.EMPLOYEES C ON (NVL (A.CREATED_BY_ID,-99) = C.EMPLOYEE_ID)
  LEFT OUTER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  LEFT OUTER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.SHIPMENT_RECEIVED_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  LEFT OUTER JOIN DW.DWDATE N ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = N.DATE_ID)
  LEFT OUTER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)
  LEFT OUTER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  LEFT OUTER JOIN DW.COST_CENTER R ON (NVL (A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  LEFT OUTER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  LEFT OUTER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW.CLASSES S ON (NVL (A.CLASS_ID,-99) = S.CLASS_ID)
WHERE A.LINE_TYPE = 'VRA_LINE'
AND   (B.VENDOR_KEY IS NULL OR 
C.EMPLOYEE_KEY IS NULL OR 
F.DATE_KEY IS NULL OR 
H.DATE_KEY IS NULL OR 
N.DATE_KEY IS NULL OR 
G.SUBSIDIARY_KEY IS NULL OR 
L.LOCATION_KEY IS NULL OR 
R.DEPARTMENT_KEY IS NULL OR 
M.ITEM_KEY IS NULL OR 
Q.TAX_ITEM_KEY IS NULL OR 
K.CURRENCY_KEY IS NULL OR 
S.CLASS_KEY IS NULL)
AND   EXISTS (SELECT 1
              FROM dw_prestage.vra_fact_update
              WHERE a.transaction_id = dw_prestage.vra_fact_update.transaction_id
              AND   a.transaction_line_id = dw_prestage.vra_fact_update.transaction_line_id);