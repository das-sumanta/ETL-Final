/* prestage - drop intermediate insert table */
DROP TABLE if exists dw_prestage.vb_fact_insert;

/* prestage - create intermediate insert table*/
CREATE TABLE dw_prestage.vb_fact_insert 
AS
SELECT *
FROM dw_prestage.vb_fact
WHERE EXISTS (SELECT 1
FROM (SELECT TRANSACTION_ID, transaction_line_id FROM (SELECT TRANSACTION_ID, transaction_line_id FROM dw_prestage.vb_fact MINUS SELECT TRANSACTION_ID, transaction_line_id FROM dw_stage.vb_fact)) a
WHERE dw_prestage.vb_fact.TRANSACTION_ID = a.TRANSACTION_ID
AND   dw_prestage.vb_fact.transaction_line_id = a.transaction_line_id
);

/* prestage - drop intermediate update table*/
DROP TABLE if exists dw_prestage.vb_fact_update;

/* prestage - create intermediate update table*/
CREATE TABLE dw_prestage.vb_fact_update 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM (SELECT TRANSACTION_ID
                  ,TRANSACTION_LINE_ID
                  ,VB_NUMBER
                  ,VENDOR_ID
                  ,REF_TRX_NUMBER
                  ,REF_TRX_TYPE
                  ,ACCOUNT_NUMBER
                  ,PAYMENT_TERMS_ID
                  ,PAYMENT_HOLD
                  ,VB_DUE_DATE
                  ,GL_POST_DATE
                  ,VENDOR_BILL_DATE
                  ,APPROVER_LEVEL_ONE_ID
                  ,APPROVER_LEVEL_TWO_ID
                  ,REQUESTOR_ID
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
                  ,VB_STATUS
                  ,APPROVAL_STATUS
                  ,ITEM_COUNT
                  ,ITEM_GROSS_AMOUNT
                  ,AMOUNT
                  ,AMOUNT_FOREIGN
                  ,NET_AMOUNT
                  ,NET_AMOUNT_FOREIGN
                  ,GROSS_AMOUNT
                  ,ITEM_UNIT_PRICE
                  ,CLOSE_DATE
                  ,TAX_ITEM_ID
                  ,TAX_AMOUNT
                  ,TAX_AMOUNT_FOREIGN
                  ,VB_TYPE
                  ,CURRENCY_ID
                  ,CLASS_ID
                  ,MATCH_EXCEPTION
                  ,EXCEPTION_MESSAGE
            FROM dw_prestage.vb_fact
            MINUS
            SELECT  TRANSACTION_ID
                  ,TRANSACTION_LINE_ID
                  ,VB_NUMBER
                  ,VENDOR_ID
                  ,REF_TRX_NUMBER
                  ,REF_TRX_TYPE
                  ,ACCOUNT_NUMBER
                  ,PAYMENT_TERMS_ID
                  ,PAYMENT_HOLD
                  ,VB_DUE_DATE
                  ,GL_POST_DATE
                  ,VENDOR_BILL_DATE
                  ,APPROVER_LEVEL_ONE_ID
                  ,APPROVER_LEVEL_TWO_ID
                  ,REQUESTOR_ID
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
                  ,VB_STATUS
                  ,APPROVAL_STATUS
                  ,ITEM_COUNT
                  ,ITEM_GROSS_AMOUNT
                  ,AMOUNT
                  ,AMOUNT_FOREIGN
                  ,NET_AMOUNT
                  ,NET_AMOUNT_FOREIGN
                  ,GROSS_AMOUNT
                  ,ITEM_UNIT_PRICE
                  ,CLOSE_DATE
                  ,TAX_ITEM_ID
                  ,TAX_AMOUNT
                  ,TAX_AMOUNT_FOREIGN
                  ,VB_TYPE
                  ,CURRENCY_ID
                  ,CLASS_ID
                  ,MATCH_EXCEPTION
                  ,EXCEPTION_MESSAGE
            FROM dw_stage.vb_fact)) a
WHERE NOT EXISTS (SELECT 1 FROM dw_prestage.vb_fact_insert WHERE dw_prestage.vb_fact_insert.TRANSACTION_ID = a.TRANSACTION_ID AND   dw_prestage.vb_fact_insert.transaction_line_id = a.transaction_line_id);

/* prestage - drop intermediate no change track table*/
DROP TABLE if exists dw_prestage.vb_fact_nochange;

/* prestage - create intermediate no change track table*/
CREATE TABLE dw_prestage.vb_fact_nochange 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vb_fact
      MINUS
      (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vb_fact_insert
      UNION ALL
      SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.vb_fact_update));

/* prestage-> stage*/
SELECT 'no of vb fact records ingested in staging -->' ||count(1)
FROM dw_prestage.vb_fact;

/* prestage-> stage*/
SELECT 'no of vb fact records identified to inserted -->' ||count(1)
FROM dw_prestage.vb_fact_insert;

/* prestage-> stage*/
SELECT 'no of vb fact records identified to updated -->' ||count(1)
FROM dw_prestage.vb_fact_update;

/* prestage-> stage*/
SELECT 'no of vb fact records identified as no change -->' ||count(1)
FROM dw_prestage.vb_fact_nochange;

--D --A = B + C + D
/* stage -> delete from stage records to be updated */ 
DELETE
FROM dw_stage.vb_fact USING dw_prestage.vb_fact_update
WHERE dw_stage.vb_fact.transaction_id = dw_prestage.vb_fact_update.transaction_id
AND   dw_stage.vb_fact.transaction_line_id = dw_prestage.vb_fact_update.transaction_line_id;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.vb_fact
SELECT *
FROM dw_prestage.vb_fact_insert;

/* stage -> insert into stage records which have been updated */ 
INSERT INTO dw_stage.vb_fact
SELECT *
FROM dw_prestage.vb_fact
WHERE EXISTS (SELECT 1
              FROM dw_prestage.vb_fact_update
              WHERE dw_prestage.vb_fact_update.transaction_id = dw_prestage.vb_fact.transaction_id
              AND   dw_prestage.vb_fact_update.transaction_line_id = dw_prestage.vb_fact.transaction_line_id);

COMMIT;

/* fact -> INSERT NEW RECORDS WHICH HAS ALL VALID DIMENSIONS */ 
INSERT INTO dw.vb_fact
(
  VB_NUMBER                  
  ,VB_ID                      
  ,VB_LINE_ID                 
  ,VENDOR_KEY                 
  ,SOURCE_TRANSACTION_NUMBER  
  ,SOURCE_TRANSACTION_TYPE    
  ,TERMS_KEY                  
  ,DUE_DATE_KEY               
  ,CREATE_DATE_KEY            
  ,ACCOUNT_KEY                
  ,PAYMENT_HOLD               
  ,GL_POST_DATE_KEY           
  ,VENDOR_BILL_DATE_KEY       
  ,REQUESTER_KEY              
  ,APPROVER_LEVEL1_KEY        
  ,APPROVER_LEVEL2_KEY        
  ,SUBSIDIARY_KEY             
  ,LOCATION_KEY               
  ,ITEM_KEY                   
  ,COST_CENTER_KEY            
  ,EXCHANGE_RATE              
  ,VENDOR_ADDRESS_LINE_1      
  ,VENDOR_ADDRESS_LINE_2      
  ,VENDOR_ADDRESS_LINE_3      
  ,VENDOR_CITY                
  ,VENDOR_COUNTRY             
  ,VENDOR_STATE               
  ,VENDOR_ZIP                 
  ,STATUS                     
  ,APPROVAL_STATUS            
  ,QUANTITY                   
  ,ITEM_GROSS_AMOUNT          
  ,AMOUNT                     
  ,AMOUNT_FOREIGN             
  ,NET_AMOUNT                 
  ,NET_AMOUNT_FOREIGN         
  ,GROSS_AMOUNT               
  ,RATE                       
  ,CLOSE_DATE_KEY             
  ,TAX_ITEM_KEY               
  ,TAX_AMOUNT                 
  ,TAX_AMOUNT_FOREIGN         
  ,VB_TYPE                    
  ,CURRENCY_KEY               
  ,CLASS_KEY                  
  ,MATCH_EXCEPTION            
  ,EXCEPTION_MESSAGE          
  ,DW_CREATION_DATE              
  ,LAST_MODIFIED_DATE         
  ,DATE_ACTIVE_FROM           
  ,DATE_ACTIVE_TO             
  ,DW_CURRENT                 
)
SELECT A.vb_number AS vb_number,
       A.TRANSACTION_ID as VB_ID,
       A.TRANSACTION_LINE_ID as VB_LINE_ID,
       B.VENDOR_KEY AS VENDOR_KEY,
       A.REF_TRX_NUMBER AS SOURCE_TRANSACTION_NUMBER,
       A.REF_TRX_TYPE AS SOURCE_TRANSACTION_TYPE,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       H.DATE_KEY AS DUE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       -99 AS ACCOUNT_KEY ,
       A.PAYMENT_HOLD AS PAYMENT_HOLD,
       I.DATE_KEY AS GL_POST_DATE_KEY,
       T.DATE_KEY AS VENDOR_BILL_DATE_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_1,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_2,       
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_3,
       A.VENDOR_CITY AS VENDOR_CITY,
       A.VENDOR_COUNTRY AS VENDOR_COUNTRY,
       A.VENDOR_STATE AS VENDOR_STATE,
       A.VENDOR_ZIP AS VENDOR_ZIP,
       A.VB_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT AS AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.ITEM_UNIT_PRICE AS RATE,
       U.DATE_KEY AS CLOSE_DATE_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.TAX_AMOUNT_FOREIGN AS TAX_AMOUNT_FOREIGN,
       A.VB_TYPE AS VB_TYPE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.MATCH_EXCEPTION as MATCH_EXCEPTION,
       A.EXCEPTION_MESSAGE as EXCEPTION_MESSAGE,
       SYSDATE AS DW_CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.vb_fact_insert A
  INNER JOIN DW_REPORT.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  INNER JOIN DW_REPORT.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  INNER JOIN DW_REPORT.DWDATE H ON (NVL (TO_CHAR (A.VB_DUE_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  INNER JOIN DW_REPORT.DWDATE I ON (NVL (TO_CHAR (A.GL_POST_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  INNER JOIN DW_REPORT.DWDATE T ON (NVL (TO_CHAR (A.VENDOR_BILL_DATE,'YYYYMMDD'),'0') = T.DATE_ID) 
  INNER JOIN DW_REPORT.DWDATE U ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = U.DATE_ID) 
  INNER JOIN DW_REPORT.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  INNER JOIN DW_REPORT.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  INNER JOIN DW_REPORT.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)   
  INNER JOIN DW_REPORT.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  INNER JOIN DW_REPORT.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW_REPORT.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)      
  INNER JOIN DW_REPORT.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW_REPORT.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW_REPORT.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  INNER JOIN DW_REPORT.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW_REPORT.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
  WHERE A.LINE_TYPE = 'VB_LINE';
  
/* fact -> INSERT NEW RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.vb_fact_error
(
  RUNID
  ,VB_NUMBER                  
  ,VB_ID                      
  ,VB_LINE_ID                 
  ,VENDOR_KEY                 
  ,SOURCE_TRANSACTION_NUMBER  
  ,SOURCE_TRANSACTION_TYPE    
  ,TERMS_KEY                  
  ,DUE_DATE_KEY               
  ,CREATE_DATE_KEY            
  ,ACCOUNT_KEY                
  ,PAYMENT_HOLD               
  ,GL_POST_DATE_KEY           
  ,VENDOR_BILL_DATE_KEY       
  ,REQUESTER_KEY              
  ,APPROVER_LEVEL1_KEY        
  ,APPROVER_LEVEL2_KEY        
  ,SUBSIDIARY_KEY             
  ,LOCATION_KEY               
  ,ITEM_KEY                   
  ,COST_CENTER_KEY            
  ,EXCHANGE_RATE              
  ,VENDOR_ADDRESS_LINE_1      
  ,VENDOR_ADDRESS_LINE_2      
  ,VENDOR_ADDRESS_LINE_3      
  ,VENDOR_CITY                
  ,VENDOR_COUNTRY             
  ,VENDOR_STATE               
  ,VENDOR_ZIP                 
  ,STATUS                     
  ,APPROVAL_STATUS            
  ,QUANTITY                   
  ,ITEM_GROSS_AMOUNT          
  ,AMOUNT                     
  ,AMOUNT_FOREIGN             
  ,NET_AMOUNT                 
  ,NET_AMOUNT_FOREIGN         
  ,GROSS_AMOUNT               
  ,RATE                       
  ,CLOSE_DATE_KEY             
  ,TAX_ITEM_KEY               
  ,TAX_AMOUNT                 
  ,TAX_AMOUNT_FOREIGN         
  ,VB_TYPE                    
  ,CURRENCY_KEY               
  ,CLASS_KEY                  
  ,MATCH_EXCEPTION            
  ,EXCEPTION_MESSAGE          
  ,CREATION_DATE              
  ,LAST_MODIFIED_DATE
  ,VENDOR_ID
  ,TERMS_ID
  ,DUE_DATE
  ,CREATE_DATE
  ,ACCOUNT_ID
  ,GL_POST_DATE
  ,VENDOR_BILL_DATE
  ,REQUESTER_ID
  ,APPROVER_LEVEL1_ID
  ,APPROVER_LEVEL2_ID
  ,SUBSIDIARY_ID
  ,LOCATION_ID
  ,ITEM_ID
  ,COST_CENTER_ID
  ,TAX_ITEM_ID
  ,CURRENCY_ID
  ,CLASS_ID
  ,RECORD_STATUS
  ,DW_CREATION_DATE
)
SELECT 
       A.RUNID ,
       A.vb_number AS vb_number,
       A.TRANSACTION_ID as VB_ID,
       A.TRANSACTION_LINE_ID as VB_LINE_ID,
       B.VENDOR_KEY AS VENDOR_KEY,
       A.REF_TRX_NUMBER AS SOURCE_TRANSACTION_NUMBER,
       A.REF_TRX_TYPE AS SOURCE_TRANSACTION_TYPE,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       H.DATE_KEY AS DUE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       -99 AS ACCOUNT_KEY ,
       A.PAYMENT_HOLD AS PAYMENT_HOLD,
       I.DATE_KEY AS GL_POST_DATE_KEY,
       T.DATE_KEY AS VENDOR_BILL_DATE_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_1,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_2,       
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_3,
       A.VENDOR_CITY AS VENDOR_CITY,
       A.VENDOR_COUNTRY AS VENDOR_COUNTRY,
       A.VENDOR_STATE AS VENDOR_STATE,
       A.VENDOR_ZIP AS VENDOR_ZIP,
       A.VB_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT AS AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.ITEM_UNIT_PRICE AS RATE,
       U.DATE_KEY AS CLOSE_DATE_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.TAX_AMOUNT_FOREIGN AS TAX_AMOUNT_FOREIGN,
       A.VB_TYPE AS VB_TYPE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.MATCH_EXCEPTION as MATCH_EXCEPTION,
       A.EXCEPTION_MESSAGE as EXCEPTION_MESSAGE,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       A.VENDOR_ID,
       A.PAYMENT_TERMS_ID,
       A.VB_DUE_DATE,
       A.CREATE_DATE,
       -99 AS ACCOUNT_ID, 
       A.GL_POST_DATE,
       A.VENDOR_BILL_DATE,
       A.REQUESTOR_ID AS REQUESTER_ID,
       A.APPROVER_LEVEL_ONE_ID AS APPROVER_LEVEL1_ID,
       A.APPROVER_LEVEL_TWO_ID AS APPROVER_LEVEL2_ID,
       A.SUBSIDIARY_ID,
       A.LOCATION_ID,
       A.ITEM_ID,
       A.DEPARTMENT_ID,
       A.TAX_ITEM_ID,
       A.CURRENCY_ID,
       A.CLASS_ID,
      'ERROR' AS RECORD_STATUS,
      SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.vb_fact_insert A
  LEFT OUTER JOIN DW_REPORT.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  LEFT OUTER JOIN DW_REPORT.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  LEFT OUTER JOIN DW_REPORT.DWDATE H ON (NVL (TO_CHAR (A.VB_DUE_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  LEFT OUTER JOIN DW_REPORT.DWDATE I ON (NVL (TO_CHAR (A.GL_POST_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  LEFT OUTER JOIN DW_REPORT.DWDATE T ON (NVL (TO_CHAR (A.VENDOR_BILL_DATE,'YYYYMMDD'),'0') = T.DATE_ID) 
  LEFT OUTER JOIN DW_REPORT.DWDATE U ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = U.DATE_ID) 
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)   
  LEFT OUTER JOIN DW_REPORT.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  LEFT OUTER JOIN DW_REPORT.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  LEFT OUTER JOIN DW_REPORT.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)      
  LEFT OUTER JOIN DW_REPORT.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW_REPORT.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW_REPORT.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  LEFT OUTER JOIN DW_REPORT.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  LEFT OUTER JOIN DW_REPORT.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
  WHERE A.LINE_TYPE = 'VB_LINE' AND 
  (B.VENDOR_KEY IS NULL OR
  C.EMPLOYEE_KEY IS NULL OR
  D.EMPLOYEE_KEY IS NULL OR
  D1.EMPLOYEE_KEY IS NULL OR
  F.DATE_KEY IS NULL OR
  H.DATE_KEY IS NULL OR
  I.DATE_KEY IS NULL OR
  T.DATE_KEY IS NULL OR
  U.DATE_KEY IS NULL OR
  G.SUBSIDIARY_KEY IS NULL OR
  L.LOCATION_KEY IS NULL OR
  R.DEPARTMENT_KEY IS NULL OR
  M.ITEM_KEY IS NULL OR
  K.CURRENCY_KEY IS NULL OR
  PAYMENT_TERM_KEY IS NULL OR
  Q.TAX_ITEM_KEY IS NULL OR
  S.CLASS_KEY IS NULL );

/* fact -> UPDATE THE OLD RECORDS SETTING THE CURRENT FLAG VALUE TO 0 */  
UPDATE dw.vb_fact SET dw_current = 0,DATE_ACTIVE_TO = (sysdate -1) WHERE dw_current = 1
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1 FROM dw_prestage.vb_fact_update WHERE dw.vb_fact.VB_ID = dw_prestage.vb_fact_update.transaction_id AND   dw.vb_fact.VB_LINE_ID = dw_prestage.vb_fact_update.transaction_line_id);

/* fact -> NOW INSERT THE FACT RECORDS WHICH HAVE BEEN UPDATED AT THE SOURCE */ 
INSERT INTO dw.vb_fact
(
   VB_NUMBER                  
  ,VB_ID                      
  ,VB_LINE_ID                 
  ,VENDOR_KEY                 
  ,SOURCE_TRANSACTION_NUMBER  
  ,SOURCE_TRANSACTION_TYPE    
  ,TERMS_KEY                  
  ,DUE_DATE_KEY               
  ,CREATE_DATE_KEY            
  ,ACCOUNT_KEY                
  ,PAYMENT_HOLD               
  ,GL_POST_DATE_KEY           
  ,VENDOR_BILL_DATE_KEY       
  ,REQUESTER_KEY              
  ,APPROVER_LEVEL1_KEY        
  ,APPROVER_LEVEL2_KEY        
  ,SUBSIDIARY_KEY             
  ,LOCATION_KEY               
  ,ITEM_KEY                   
  ,COST_CENTER_KEY            
  ,EXCHANGE_RATE              
  ,VENDOR_ADDRESS_LINE_1      
  ,VENDOR_ADDRESS_LINE_2      
  ,VENDOR_ADDRESS_LINE_3      
  ,VENDOR_CITY                
  ,VENDOR_COUNTRY             
  ,VENDOR_STATE               
  ,VENDOR_ZIP                 
  ,STATUS                     
  ,APPROVAL_STATUS            
  ,QUANTITY                   
  ,ITEM_GROSS_AMOUNT          
  ,AMOUNT                     
  ,AMOUNT_FOREIGN             
  ,NET_AMOUNT                 
  ,NET_AMOUNT_FOREIGN         
  ,GROSS_AMOUNT               
  ,RATE                       
  ,CLOSE_DATE_KEY             
  ,TAX_ITEM_KEY               
  ,TAX_AMOUNT                 
  ,TAX_AMOUNT_FOREIGN         
  ,VB_TYPE                    
  ,CURRENCY_KEY               
  ,CLASS_KEY                  
  ,MATCH_EXCEPTION            
  ,EXCEPTION_MESSAGE          
  ,DW_CREATION_DATE              
  ,LAST_MODIFIED_DATE         
  ,DATE_ACTIVE_FROM           
  ,DATE_ACTIVE_TO             
  ,DW_CURRENT 
)
SELECT A.vb_number AS vb_number,
       A.TRANSACTION_ID as VB_ID,
       A.TRANSACTION_LINE_ID as VB_LINE_ID,
       B.VENDOR_KEY AS VENDOR_KEY,
       A.REF_TRX_NUMBER AS SOURCE_TRANSACTION_NUMBER,
       A.REF_TRX_TYPE AS SOURCE_TRANSACTION_TYPE,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       H.DATE_KEY AS DUE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       -99 AS ACCOUNT_KEY ,
       A.PAYMENT_HOLD AS PAYMENT_HOLD,
       I.DATE_KEY AS GL_POST_DATE_KEY,
       T.DATE_KEY AS VENDOR_BILL_DATE_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_1,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_2,       
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_3,
       A.VENDOR_CITY AS VENDOR_CITY,
       A.VENDOR_COUNTRY AS VENDOR_COUNTRY,
       A.VENDOR_STATE AS VENDOR_STATE,
       A.VENDOR_ZIP AS VENDOR_ZIP,
       A.VB_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT AS AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.ITEM_UNIT_PRICE AS RATE,
       U.DATE_KEY AS CLOSE_DATE_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.TAX_AMOUNT_FOREIGN AS TAX_AMOUNT_FOREIGN,
       A.VB_TYPE AS VB_TYPE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.MATCH_EXCEPTION as MATCH_EXCEPTION,
       A.EXCEPTION_MESSAGE as EXCEPTION_MESSAGE,
       SYSDATE AS DW_CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.vb_fact A
  INNER JOIN DW_REPORT.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  INNER JOIN DW_REPORT.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  INNER JOIN DW_REPORT.DWDATE H ON (NVL (TO_CHAR (A.VB_DUE_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  INNER JOIN DW_REPORT.DWDATE I ON (NVL (TO_CHAR (A.GL_POST_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  INNER JOIN DW_REPORT.DWDATE T ON (NVL (TO_CHAR (A.VENDOR_BILL_DATE,'YYYYMMDD'),'0') = T.DATE_ID) 
  INNER JOIN DW_REPORT.DWDATE U ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = U.DATE_ID) 
  INNER JOIN DW_REPORT.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  INNER JOIN DW_REPORT.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  INNER JOIN DW_REPORT.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)   
  INNER JOIN DW_REPORT.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  INNER JOIN DW_REPORT.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW_REPORT.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)      
  INNER JOIN DW_REPORT.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW_REPORT.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW_REPORT.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  INNER JOIN DW_REPORT.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW_REPORT.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
WHERE A.LINE_TYPE = 'VB_LINE'
AND   EXISTS (SELECT 1 FROM dw_prestage.vb_fact_update WHERE a.transaction_id = dw_prestage.vb_fact_update.transaction_id AND   a.transaction_line_id = dw_prestage.vb_fact_update.transaction_line_id);

/* fact -> INSERT UPDATED RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.vb_fact_error
(
  RUNID
  ,VB_NUMBER                  
  ,VB_ID                      
  ,VB_LINE_ID                 
  ,VENDOR_KEY                 
  ,SOURCE_TRANSACTION_NUMBER  
  ,SOURCE_TRANSACTION_TYPE    
  ,TERMS_KEY                  
  ,DUE_DATE_KEY               
  ,CREATE_DATE_KEY            
  ,ACCOUNT_KEY                
  ,PAYMENT_HOLD               
  ,GL_POST_DATE_KEY           
  ,VENDOR_BILL_DATE_KEY       
  ,REQUESTER_KEY              
  ,APPROVER_LEVEL1_KEY        
  ,APPROVER_LEVEL2_KEY        
  ,SUBSIDIARY_KEY             
  ,LOCATION_KEY               
  ,ITEM_KEY                   
  ,COST_CENTER_KEY            
  ,EXCHANGE_RATE              
  ,VENDOR_ADDRESS_LINE_1      
  ,VENDOR_ADDRESS_LINE_2      
  ,VENDOR_ADDRESS_LINE_3      
  ,VENDOR_CITY                
  ,VENDOR_COUNTRY             
  ,VENDOR_STATE               
  ,VENDOR_ZIP                 
  ,STATUS                     
  ,APPROVAL_STATUS            
  ,QUANTITY                   
  ,ITEM_GROSS_AMOUNT          
  ,AMOUNT                     
  ,AMOUNT_FOREIGN             
  ,NET_AMOUNT                 
  ,NET_AMOUNT_FOREIGN         
  ,GROSS_AMOUNT               
  ,RATE                       
  ,CLOSE_DATE_KEY             
  ,TAX_ITEM_KEY               
  ,TAX_AMOUNT                 
  ,TAX_AMOUNT_FOREIGN         
  ,VB_TYPE                    
  ,CURRENCY_KEY               
  ,CLASS_KEY                  
  ,MATCH_EXCEPTION            
  ,EXCEPTION_MESSAGE          
  ,CREATION_DATE              
  ,LAST_MODIFIED_DATE
  ,VENDOR_ID
  ,TERMS_ID
  ,DUE_DATE
  ,CREATE_DATE
  ,ACCOUNT_ID
  ,GL_POST_DATE
  ,VENDOR_BILL_DATE
  ,REQUESTER_ID
  ,APPROVER_LEVEL1_ID
  ,APPROVER_LEVEL2_ID
  ,SUBSIDIARY_ID
  ,LOCATION_ID
  ,ITEM_ID
  ,COST_CENTER_ID
  ,TAX_ITEM_ID
  ,CURRENCY_ID
  ,CLASS_ID
  ,RECORD_STATUS
  ,DW_CREATION_DATE
)
SELECT A.RUNID ,
       A.vb_number AS vb_number,
       A.TRANSACTION_ID as VB_ID,
       A.TRANSACTION_LINE_ID as VB_LINE_ID,
       B.VENDOR_KEY AS VENDOR_KEY,
       A.REF_TRX_NUMBER AS SOURCE_TRANSACTION_NUMBER,
       A.REF_TRX_TYPE AS SOURCE_TRANSACTION_TYPE,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       H.DATE_KEY AS DUE_DATE_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       -99 AS ACCOUNT_KEY ,
       A.PAYMENT_HOLD AS PAYMENT_HOLD,
       I.DATE_KEY AS GL_POST_DATE_KEY,
       T.DATE_KEY AS VENDOR_BILL_DATE_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_1,
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_2,       
       A.VENDOR_ADDRESS_LINE_1 AS VENDOR_ADDRESS_LINE_3,
       A.VENDOR_CITY AS VENDOR_CITY,
       A.VENDOR_COUNTRY AS VENDOR_COUNTRY,
       A.VENDOR_STATE AS VENDOR_STATE,
       A.VENDOR_ZIP AS VENDOR_ZIP,
       A.VB_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.ITEM_COUNT AS QUANTITY,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT AS AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.ITEM_UNIT_PRICE AS RATE,
       U.DATE_KEY AS CLOSE_DATE_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.TAX_AMOUNT_FOREIGN AS TAX_AMOUNT_FOREIGN,
       A.VB_TYPE AS VB_TYPE,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       A.MATCH_EXCEPTION as MATCH_EXCEPTION,
       A.EXCEPTION_MESSAGE as EXCEPTION_MESSAGE,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       A.VENDOR_ID,
       A.PAYMENT_TERMS_ID,
       A.VB_DUE_DATE,
       A.CREATE_DATE,
       -99 AS ACCOUNT_ID, 
       A.GL_POST_DATE,
       A.VENDOR_BILL_DATE,
       A.REQUESTOR_ID AS REQUESTER_ID,
       A.APPROVER_LEVEL_ONE_ID AS APPROVER_LEVEL1_ID,
       A.APPROVER_LEVEL_TWO_ID AS APPROVER_LEVEL2_ID,
       A.SUBSIDIARY_ID,
       A.LOCATION_ID,
       A.ITEM_ID,
       A.DEPARTMENT_ID,
       A.TAX_ITEM_ID,
       A.CURRENCY_ID,
       A.CLASS_ID,
      'ERROR' AS RECORD_STATUS,
      SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.vb_fact A
  LEFT OUTER JOIN DW_REPORT.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  LEFT OUTER JOIN DW_REPORT.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)
  LEFT OUTER JOIN DW_REPORT.DWDATE H ON (NVL (TO_CHAR (A.VB_DUE_DATE,'YYYYMMDD'),'0') = H.DATE_ID)
  LEFT OUTER JOIN DW_REPORT.DWDATE I ON (NVL (TO_CHAR (A.GL_POST_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  LEFT OUTER JOIN DW_REPORT.DWDATE T ON (NVL (TO_CHAR (A.VENDOR_BILL_DATE,'YYYYMMDD'),'0') = T.DATE_ID) 
  LEFT OUTER JOIN DW_REPORT.DWDATE U ON (NVL (TO_CHAR (A.CLOSE_DATE,'YYYYMMDD'),'0') = U.DATE_ID) 
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW_REPORT.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)   
  LEFT OUTER JOIN DW_REPORT.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  LEFT OUTER JOIN DW_REPORT.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  LEFT OUTER JOIN DW_REPORT.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)      
  LEFT OUTER JOIN DW_REPORT.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW_REPORT.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW_REPORT.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  LEFT OUTER JOIN DW_REPORT.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  LEFT OUTER JOIN DW_REPORT.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
  WHERE A.LINE_TYPE = 'VB_LINE' AND 
  (B.VENDOR_KEY IS NULL OR
  C.EMPLOYEE_KEY IS NULL OR
  D.EMPLOYEE_KEY IS NULL OR
  D1.EMPLOYEE_KEY IS NULL OR
  F.DATE_KEY IS NULL OR
  H.DATE_KEY IS NULL OR
  I.DATE_KEY IS NULL OR
  T.DATE_KEY IS NULL OR
  U.DATE_KEY IS NULL OR
  G.SUBSIDIARY_KEY IS NULL OR
  L.LOCATION_KEY IS NULL OR
  R.DEPARTMENT_KEY IS NULL OR
  M.ITEM_KEY IS NULL OR
  K.CURRENCY_KEY IS NULL OR
  PAYMENT_TERM_KEY IS NULL OR
  Q.TAX_ITEM_KEY IS NULL OR
  S.CLASS_KEY IS NULL )
AND   EXISTS (SELECT 1 
             FROM dw_prestage.vb_fact_update 
             WHERE 
                   a.transaction_id = dw_prestage.vb_fact_update.transaction_id 
             AND   a.transaction_line_id = dw_prestage.vb_fact_update.transaction_line_id);