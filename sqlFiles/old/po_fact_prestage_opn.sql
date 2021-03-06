/* prestage - drop intermediate insert table */
DROP TABLE if exists dw_prestage.po_fact_insert;

/* prestage - create intermediate insert table*/
CREATE TABLE dw_prestage.po_fact_insert 
AS
SELECT *
FROM dw_prestage.po_fact
WHERE EXISTS (SELECT 1
FROM (SELECT TRANSACTION_ID, transaction_line_id FROM (SELECT TRANSACTION_ID, transaction_line_id FROM dw_prestage.po_fact MINUS SELECT TRANSACTION_ID, transaction_line_id FROM dw_stage.po_fact)) a
WHERE dw_prestage.po_fact.TRANSACTION_ID = a.TRANSACTION_ID
AND   dw_prestage.po_fact.transaction_line_id = a.transaction_line_id
);

/* prestage - drop intermediate update table*/
DROP TABLE if exists dw_prestage.po_fact_update;

/* prestage - create intermediate update table*/
CREATE TABLE dw_prestage.po_fact_update 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM (SELECT TRANSACTION_ID,
                   transaction_line_id,
                   REF_TRX_NUMBER,  /* ADDED */
                   REF_TRX_TYPE,   /* ADDED */
                   PO_NUMBER,
                   VENDOR_ID,
                   APPROVER_LEVEL_ONE_ID,
                   APPROVER_LEVEL_TWO_ID,
                   APPROVAL_STATUS,
                   BILL_ADDRESS_LINE_1,  /* ADDED */
                   BILL_ADDRESS_LINE_2, /* ADDED */
                   BILL_ADDRESS_LINE_3, /* ADDED */
                   BILL_CITY,        /* ADDED */   
                   BILL_COUNTRY,     /* ADDED */   
                   BILL_STATE,        /* ADDED */  
                   BILL_ZIP,         /* ADDED */   
                   CARRIER_ID,  /* MODIFIED */
                   REQUESTOR_ID,
                   CREATE_DATE,
                   CURRENCY_ID,
                   CUSTOM_FORM_ID,
                   EXCHANGE_RATE,
                   LOCATION_ID,
                   SHIP_ADDRESS_LINE_1,    /* ADDD */ 
                   SHIP_ADDRESS_LINE_2 ,   /* ADDD */ 
                   SHIP_ADDRESS_LINE_3,    /* ADDD */ 
                   SHIP_CITY    ,          /* ADDD */ 
                   SHIP_COUNTRY,           /* ADDD */ 
                   SHIP_STATE  ,           /* ADDD */ 
                   SHIP_ZIP   ,            /* ADDD */ 
                   PO_STATUS,
                   PAYMENT_TERMS_ID,
                   FRIGHT_RATE,
                   PO_TYPE,
                   SUBSIDIARY_ID,
                   DEPARTMENT_ID,
                   ITEM_ID,
                   BC_QUANTITY,
                   BIH_QUANTITY,
                   BOOK_FAIR_QUANTITY,
                   EDUCATION_QUANTITY,
                   NZSO_QUANTITY,
                   TRADE_QUANTITY,
                   SCHOOL_ESSENTIALS_QUANTITY,
                   ITEM_COUNT,
                   ITEM_GROSS_AMOUNT,
                   ITEM_UNIT_PRICE,
                   NUMBER_BILLED,                  /* ADDD */ 
                   QUANTITY_RECEIVED_IN_SHIPMENT,  /* ADDD */ 
                   QUANTITY_RETURNED,              /* ADDD */ 
                   EXPECTED_RECEIPT_DATE,
                   ACTUAL_DELIVERY_DATE,
                   TAX_ITEM_ID,
                   FREIGHT_ESTIMATE_METHOD_ID,
                   AMOUNT,
                   AMOUNT_FOREIGN,
                   NET_AMOUNT,
                   NET_AMOUNT_FOREIGN,
                   GROSS_AMOUNT,
                   MATCH_BILL_TO_RECEIPT,
                   TRACK_LANDED_COST,
                   TAX_AMOUNT,
                   CLASS_ID
            FROM dw_prestage.po_fact
            MINUS
            SELECT TRANSACTION_ID,
                   transaction_line_id,
                   REF_TRX_NUMBER,  /* ADDED */
                   REF_TRX_TYPE,   /* ADDED */
                   PO_NUMBER,
                   VENDOR_ID,
                   APPROVER_LEVEL_ONE_ID,
                   APPROVER_LEVEL_TWO_ID,
                   APPROVAL_STATUS,
                   BILL_ADDRESS_LINE_1,  /* ADDED */
                   BILL_ADDRESS_LINE_2, /* ADDED */
                   BILL_ADDRESS_LINE_3, /* ADDED */
                   BILL_CITY,        /* ADDED */   
                   BILL_COUNTRY,     /* ADDED */   
                   BILL_STATE,        /* ADDED */  
                   BILL_ZIP,         /* ADDED */   
                   CARRIER_ID,  /* MODIFIED */
                   REQUESTOR_ID,
                   CREATE_DATE,
                   CURRENCY_ID,
                   CUSTOM_FORM_ID,
                   EXCHANGE_RATE,
                   LOCATION_ID,
                   SHIP_ADDRESS_LINE_1,    /* ADDD */ 
                   SHIP_ADDRESS_LINE_2 ,   /* ADDD */ 
                   SHIP_ADDRESS_LINE_3,    /* ADDD */ 
                   SHIP_CITY    ,          /* ADDD */ 
                   SHIP_COUNTRY,           /* ADDD */ 
                   SHIP_STATE  ,           /* ADDD */ 
                   SHIP_ZIP   ,            /* ADDD */ 
                   PO_STATUS,
                   PAYMENT_TERMS_ID,
                   FRIGHT_RATE,
                   PO_TYPE,
                   SUBSIDIARY_ID,
                   DEPARTMENT_ID,
                   ITEM_ID,
                   BC_QUANTITY,
                   BIH_QUANTITY,
                   BOOK_FAIR_QUANTITY,
                   EDUCATION_QUANTITY,
                   NZSO_QUANTITY,
                   TRADE_QUANTITY,
                   SCHOOL_ESSENTIALS_QUANTITY,
                   ITEM_COUNT,
                   ITEM_GROSS_AMOUNT,
                   ITEM_UNIT_PRICE,
                   NUMBER_BILLED,                  /* ADDD */ 
                   QUANTITY_RECEIVED_IN_SHIPMENT,  /* ADDD */ 
                   QUANTITY_RETURNED,              /* ADDD */ 
                   EXPECTED_RECEIPT_DATE,
                   ACTUAL_DELIVERY_DATE,
                   TAX_ITEM_ID,
                   FREIGHT_ESTIMATE_METHOD_ID,
                   AMOUNT,
                   AMOUNT_FOREIGN,
                   NET_AMOUNT,
                   NET_AMOUNT_FOREIGN,
                   GROSS_AMOUNT,
                   MATCH_BILL_TO_RECEIPT,
                   TRACK_LANDED_COST,
                   TAX_AMOUNT,
                   CLASS_ID
            FROM dw_stage.po_fact)) a
WHERE NOT EXISTS (SELECT 1 FROM dw_prestage.po_fact_insert WHERE dw_prestage.po_fact_insert.TRANSACTION_ID = a.TRANSACTION_ID AND   dw_prestage.po_fact_insert.transaction_line_id = a.transaction_line_id);

/* prestage - drop intermediate no change track table*/
DROP TABLE if exists dw_prestage.po_fact_nochange;

/* prestage - create intermediate no change track table*/
CREATE TABLE dw_prestage.po_fact_nochange 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.po_fact
      MINUS
      (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.po_fact_insert
      UNION ALL
      SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.po_fact_update));

/* prestage-> stage*/
SELECT 'no of po fact records ingested in staging -->' ||count(1)
FROM dw_prestage.po_fact;

/* prestage-> stage*/
SELECT 'no of po fact records identified to inserted -->' ||count(1)
FROM dw_prestage.po_fact_insert;

/* prestage-> stage*/
SELECT 'no of po fact records identified to updated -->' ||count(1)
FROM dw_prestage.po_fact_update;

/* prestage-> stage*/
SELECT 'no of po fact records identified as no change -->' ||count(1)
FROM dw_prestage.po_fact_nochange;

--D --A = B + C + D
/* stage -> delete from stage records to be updated */ 
DELETE
FROM dw_stage.po_fact USING dw_prestage.po_fact_update
WHERE dw_stage.po_fact.transaction_id = dw_prestage.po_fact_update.transaction_id
AND   dw_stage.po_fact.transaction_line_id = dw_prestage.po_fact_update.transaction_line_id;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.po_fact
SELECT *
FROM dw_prestage.po_fact_insert;

/* stage -> insert into stage records which have been updated */ 
INSERT INTO dw_stage.po_fact
SELECT *
FROM dw_prestage.po_fact
WHERE EXISTS (SELECT 1
              FROM dw_prestage.po_fact_update
              WHERE dw_prestage.po_fact_update.transaction_id = dw_prestage.po_fact.transaction_id
              AND   dw_prestage.po_fact_update.transaction_line_id = dw_prestage.po_fact.transaction_line_id);

COMMIT;

/* fact -> INSERT NEW RECORDS WHICH HAS ALL VALID DIMENSIONS */ 
INSERT INTO dw.po_fact
(
  PO_NUMBER,
  PO_ID ,                  /* ADDD */  
  PO_LINE_ID  ,            /* ADDD */  
  REF_TRX_NUMBER ,         /* ADDD */  
  REF_TRX_TYPE ,           /* ADDD */  
  VENDOR_KEY  ,
  REQUESTER_KEY  ,
  APPROVER_LEVEL1_KEY  ,
  APPROVER_LEVEL2_KEY  ,
  CREATE_DATE_KEY  ,
  SUBSIDIARY_KEY  ,
  LOCATION_KEY  ,
  COST_CENTER_KEY  ,
  ITEM_KEY  ,
  REQUESTED_RECEIPT_DATE_KEY  ,
  ACTUAL_DELIVERY_DATE_KEY  ,
  FREIGHT_ESTIMATE_METHOD_KEY  ,
  TERMS_KEY  ,
  TAX_ITEM_KEY  ,
  CURRENCY_KEY  ,
  CLASS_KEY ,
  CARRIER_KEY,               /* ADDD */
  EXCHANGE_RATE,
  BILL_ADDRESS_LINE_1 ,        /* ADDD */
  BILL_ADDRESS_LINE_2 ,        /* ADDD */
  BILL_ADDRESS_LINE_3 ,        /* ADDD */
  BILL_CITY           ,        /* ADDD */
  BILL_COUNTRY        ,        /* ADDD */
  BILL_STATE          ,        /* ADDD */
  BILL_ZIP            ,        /* ADDD */
  SHIP_ADDRESS_LINE_1 ,        /* ADDD */
  SHIP_ADDRESS_LINE_2 ,        /* ADDD */
  SHIP_ADDRESS_LINE_3 ,        /* ADDD */
  SHIP_CITY           ,        /* ADDD */
  SHIP_COUNTRY        ,        /* ADDD */
  SHIP_STATE          ,        /* ADDD */
  SHIP_ZIP            ,        /* ADDD */
  QUANTITY,
  BIH_QUANTITY,
  BC_QUANTITY,
  TRADE_QUANTITY,
  NZSO_QUANTITY,
  EDUCATION_QUANTITY,
  SCHOOL_ESSENTIALS_QUANTITY,
  BOOK_FAIR_QUANTITY,  
  NUMBER_BILLED   ,                  /* ADDD */ 
  QUANTITY_RECEIVED_IN_SHIPMENT   ,  /* ADDD */ 
  QUANTITY_RETURNED  ,               /* ADDD */ 
  RATE,
  AMOUNT,
  ITEM_GROSS_AMOUNT,
  AMOUNT_FOREIGN,
  NET_AMOUNT,
  NET_AMOUNT_FOREIGN,
  GROSS_AMOUNT,
  MATCH_BILL_TO_RECEIPT,
  TRACK_LANDED_COST,
  TAX_AMOUNT,
  FREIGHT_RATE,
  PO_TYPE,
  STATUS,
  APPROVAL_STATUS,
  CREATION_DATE,
  LAST_MODIFIED_DATE,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_CURRENT
)
SELECT A.po_number AS po_number, 
       A.transaction_id AS po_id,            /* added */
       A.transaction_line_id AS po_line_id,  /* added */
       A.ref_trx_number AS ref_trx_number,   /* added */
       A.ref_trx_type AS ref_trx_type,       /* added */
       B.VENDOR_KEY AS VENDOR_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       H.DATE_KEY AS REQUESTED_RECEIPT_DATE_KEY,
       I.DATE_KEY AS ACTUAL_DELIVERY_DATE_KEY,
       J.FREIGHT_KEY AS FREIGHT_ESTIMATE_METHOD_KEY,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       T.CARRIER_KEY AS CARRIER_KEY,        /* added */
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.BILL_ADDRESS_LINE_1 ,         /* added */
       A.BILL_ADDRESS_LINE_2 ,         /* added */
       A.BILL_ADDRESS_LINE_3 ,         /* added */
       A.BILL_CITY           ,         /* added */
       A.BILL_COUNTRY        ,          /* added */
       A.BILL_STATE          ,         /* added */
       A.BILL_ZIP            ,          /* added */
       A.SHIP_ADDRESS_LINE_1 ,         /* added */
       A.SHIP_ADDRESS_LINE_2 ,         /* added */
       A.SHIP_ADDRESS_LINE_3 ,         /* added */
       A.SHIP_CITY           ,         /* added */
       A.SHIP_COUNTRY        ,         /* added */
       A.SHIP_STATE          ,         /* added */
       A.SHIP_ZIP            ,         /* added */
       A.ITEM_COUNT AS QUANTITY,
       A.BIH_QUANTITY AS BIH_QUANTITY,
       A.BC_QUANTITY AS BC_QUANTITY,
       A.TRADE_QUANTITY AS TRADE_QUANTITY,
       A.NZSO_QUANTITY AS NZSO_QUANTITY,
       A.EDUCATION_QUANTITY AS EDUCATION_QUANTITY,
       A.SCHOOL_ESSENTIALS_QUANTITY AS SCHOOL_ESSENTIALS_QUANTITY,
       A.BOOK_FAIR_QUANTITY AS BOOK_FAIR_QUANTITY,
       A.NUMBER_BILLED  ,                  /* added */
       A.QUANTITY_RECEIVED_IN_SHIPMENT  ,  /* added */
       A.QUANTITY_RETURNED       ,         /* added */
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.MATCH_BILL_TO_RECEIPT as MATCH_BILL_TO_RECEIPT,
       A.TRACK_LANDED_COST as TRACK_LANDED_COST,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.FRIGHT_RATE AS FREIGHT_RATE,
       A.PO_TYPE AS PO_TYPE,
       A.PO_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.po_fact_insert A
  INNER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  INNER JOIN DW.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  INNER JOIN DW.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  INNER JOIN DW.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)  
  INNER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)  
  INNER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  INNER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.EXPECTED_RECEIPT_DATE,'YYYYMMDD'),'0') = H.DATE_ID)  
  INNER JOIN DW.DWDATE I ON (NVL (TO_CHAR (A.ACTUAL_DELIVERY_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  INNER JOIN DW.FREIGHT_ESTIMATE J ON (NVL (A.FREIGHT_ESTIMATE_METHOD_ID,-99) = J.LANDED_COST_RULE_MATRIX_NZ_ID)  
  INNER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  INNER JOIN DW.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  INNER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
  INNER JOIN DW.CARRIER T ON (NVL(A.CARRIER_ID,-99) = T.CARRIER_ID)
  WHERE A.LINE_TYPE = 'PO_LINE';
  
/* fact -> INSERT NEW RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.po_fact_error
(
RUNID,
PO_NUMBER,
PO_ID ,                  /* ADDD */  
PO_LINE_ID  ,            /* ADDD */  
REF_TRX_NUMBER ,         /* ADDD */  
REF_TRX_TYPE ,           /* ADDD */  
VENDOR_KEY,
REQUESTER_KEY,
APPROVER_LEVEL1_KEY,
APPROVER_LEVEL2_KEY,
CREATE_DATE_KEY,
SUBSIDIARY_KEY,
LOCATION_KEY,
COST_CENTER_KEY,
EXCHANGE_RATE,
ITEM_KEY,
BILL_ADDRESS_LINE_1 ,        /* ADDD */ 
BILL_ADDRESS_LINE_2 ,        /* ADDD */ 
BILL_ADDRESS_LINE_3 ,        /* ADDD */ 
BILL_CITY           ,        /* ADDD */ 
BILL_COUNTRY        ,        /* ADDD */ 
BILL_STATE          ,        /* ADDD */ 
BILL_ZIP            ,        /* ADDD */ 
SHIP_ADDRESS_LINE_1 ,        /* ADDD */ 
SHIP_ADDRESS_LINE_2 ,        /* ADDD */ 
SHIP_ADDRESS_LINE_3 ,        /* ADDD */ 
SHIP_CITY           ,        /* ADDD */ 
SHIP_COUNTRY        ,        /* ADDD */ 
SHIP_STATE          ,        /* ADDD */ 
SHIP_ZIP            ,        /* ADDD */ 
QUANTITY,
BIH_QUANTITY,
BC_QUANTITY,
TRADE_QUANTITY,
NZSO_QUANTITY,
EDUCATION_QUANTITY,
SCHOOL_ESSENTIALS_QUANTITY,
BOOK_FAIR_QUANTITY,
NUMBER_BILLED   ,                  /* ADDD */ 
QUANTITY_RECEIVED_IN_SHIPMENT   ,  /* ADDD */ 
QUANTITY_RETURNED  ,               /* ADDD */ 
RATE,
AMOUNT,
ITEM_GROSS_AMOUNT,
AMOUNT_FOREIGN,
NET_AMOUNT,
NET_AMOUNT_FOREIGN,
GROSS_AMOUNT,
MATCH_BILL_TO_RECEIPT,
TRACK_LANDED_COST,
TAX_ITEM_KEY,
TAX_AMOUNT,
REQUESTED_RECEIPT_DATE_KEY,
ACTUAL_DELIVERY_DATE_KEY,
FREIGHT_ESTIMATE_METHOD_KEY,
FREIGHT_RATE,
TERMS_KEY,
PO_TYPE,
STATUS,
APPROVAL_STATUS,
CREATION_DATE,
LAST_MODIFIED_DATE,
CURRENCY_KEY,
CLASS_KEY,  
CARRIER_KEY,               /* ADDD */ 
CARRIER_ID ,               /* ADDD */ 
VENDOR_ID,
REQUESTER_ID,
APPROVER_LEVEL1_ID,
APPROVER_LEVEL2_ID,
SUBSIDIARY_ID,
LOCATION_ID,
COST_CENTER_ID,
ITEM_ID,
TAX_ITEM_ID,
REQUESTED_RECEIPT_DATE,
ACTUAL_DELIVERY_DATE,
FREIGHT_ESTIMATE_METHOD_ID,
TERMS_ID,
CURRENCY_ID,
CLASS_ID,
RECORD_STATUS,
DW_CREATION_DATE
)
SELECT 
A.RUNID ,
A.PO_NUMBER,
A.transaction_id AS po_id,            /* added */
A.transaction_line_id AS po_line_id,  /* added */
A.ref_trx_number AS ref_trx_number,   /* added */
A.ref_trx_type AS ref_trx_type,       /* added */
B.VENDOR_KEY,
C.EMPLOYEE_KEY AS REQUESTER_KEY,
D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
D.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
F.DATE_KEY AS CREATE_DATE_KEY,
G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
L.LOCATION_KEY AS LOCATION_KEY,
R.DEPARTMENT_KEY AS COST_CENTER_KEY,
A.EXCHANGE_RATE,
M.ITEM_KEY AS ITEM_KEY,
A.BILL_ADDRESS_LINE_1 ,         /* added */  
A.BILL_ADDRESS_LINE_2 ,         /* added */  
A.BILL_ADDRESS_LINE_3 ,         /* added */  
A.BILL_CITY           ,         /* added */  
A.BILL_COUNTRY        ,          /* added */ 
A.BILL_STATE          ,         /* added */  
A.BILL_ZIP            ,          /* added */ 
A.SHIP_ADDRESS_LINE_1 ,         /* added */  
A.SHIP_ADDRESS_LINE_2 ,         /* added */  
A.SHIP_ADDRESS_LINE_3 ,         /* added */  
A.SHIP_CITY           ,         /* added */  
A.SHIP_COUNTRY        ,         /* added */  
A.SHIP_STATE          ,         /* added */  
A.SHIP_ZIP            ,         /* added */  
A.ITEM_COUNT AS QUANTITY,
A.BIH_QUANTITY,
A.BC_QUANTITY,
A.TRADE_QUANTITY,
A.NZSO_QUANTITY,
A.EDUCATION_QUANTITY,
A.SCHOOL_ESSENTIALS_QUANTITY,
A.BOOK_FAIR_QUANTITY,
A.NUMBER_BILLED  ,                  /* added */
A.QUANTITY_RECEIVED_IN_SHIPMENT  ,  /* added */
A.QUANTITY_RETURNED       ,         /* added */
A.ITEM_UNIT_PRICE AS RATE,
A.AMOUNT,
A.ITEM_GROSS_AMOUNT,
A.AMOUNT_FOREIGN,
A.NET_AMOUNT,
A.NET_AMOUNT_FOREIGN,
A.GROSS_AMOUNT,
A.MATCH_BILL_TO_RECEIPT,
A.TRACK_LANDED_COST,
Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
A.TAX_AMOUNT,
H.DATE_KEY AS REQUESTED_RECEIPT_DATE_KEY,
I.DATE_KEY AS ACTUAL_DELIVERY_DATE_KEY,
J.FREIGHT_KEY AS FREIGHT_ESTIMATE_METHOD_KEY,
A.FRIGHT_RATE AS FREIGHT_RATE,
P.PAYMENT_TERM_KEY AS TERMS_KEY,
A.PO_TYPE,
A.PO_STATUS AS STATUS,
A.APPROVAL_STATUS,
A.CREATE_DATE AS CREATION_DATE,
A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
K.CURRENCY_KEY AS CURRENCY_KEY,
S.CLASS_KEY AS CLASS_KEY ,
T.CARRIER_KEY AS CARRIER_KEY,        /* added */
A.CARRIER_ID ,                        /* added */
A.VENDOR_ID ,
A.REQUESTOR_ID ,
A.APPROVER_LEVEL_ONE_ID ,
A.APPROVER_LEVEL_TWO_ID ,
A.SUBSIDIARY_ID ,
A.LOCATION_ID ,
A.DEPARTMENT_ID AS COST_CENTER_ID,
A.ITEM_ID,
A.TAX_ITEM_ID,
A.EXPECTED_RECEIPT_DATE,
A.ACTUAL_DELIVERY_DATE,
A.FREIGHT_ESTIMATE_METHOD_ID,
A.PAYMENT_TERMS_ID AS TERMS_ID,
A.CURRENCY_ID,
A.CLASS_ID,
'ERROR' AS RECORD_STATUS,
SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.po_fact_insert A
  LEFT OUTER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,0) = B.VENDOR_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,0) = C.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,0) = D.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,0) = D1.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)  
  LEFT OUTER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,0) = G.SUBSIDIARY_ID)  
  LEFT OUTER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.EXPECTED_RECEIPT_DATE,'YYYYMMDD'),'0') = H.DATE_ID)  
  LEFT OUTER JOIN DW.DWDATE I ON (NVL (TO_CHAR (A.ACTUAL_DELIVERY_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  LEFT OUTER JOIN DW.FREIGHT_ESTIMATE J ON (NVL (A.FREIGHT_ESTIMATE_METHOD_ID,0) = J.LANDED_COST_RULE_MATRIX_NZ_ID)  
  LEFT OUTER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,0) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,0) = L.LOCATION_ID)
  LEFT OUTER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,0) = M.ITEM_ID)
  LEFT OUTER JOIN DW.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,0) = P.PAYMENT_TERMS_ID)
  LEFT OUTER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,0) = Q.ITEM_ID)
  LEFT OUTER JOIN DW.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,0) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW.CLASSES S ON (NVL(A.CLASS_ID,0) = S.CLASS_ID)
  LEFT OUTER JOIN DW.CARRIER T ON (NVL(A.CARRIER_ID,-99) = T.CARRIER_ID)
  WHERE A.LINE_TYPE = 'PO_LINE' AND 
  (B.VENDOR_KEY IS NULL OR
  C.EMPLOYEE_KEY IS NULL OR
  D.EMPLOYEE_KEY IS NULL OR
  D1.EMPLOYEE_KEY IS NULL OR
  F.DATE_KEY IS NULL OR
  G.SUBSIDIARY_KEY IS NULL OR
  L.LOCATION_KEY IS NULL OR
  R.DEPARTMENT_KEY IS NULL OR
  M.ITEM_KEY IS NULL OR
  Q.TAX_ITEM_KEY IS NULL OR
  H.DATE_KEY IS NULL OR
  I.DATE_KEY IS NULL OR
  J.FREIGHT_KEY IS NULL OR
  P.PAYMENT_TERM_KEY IS NULL OR
  K.CURRENCY_KEY IS NULL OR
  S.CLASS_KEY IS NULL OR
  T.CARRIER_KEY IS NULL );

/* fact -> UPDATE THE OLD RECORDS SETTING THE CURRENT FLAG VALUE TO 0 */  
UPDATE dw.po_fact SET dw_current = 0,DATE_ACTIVE_TO = (sysdate -1) WHERE dw_current = 1
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1 FROM dw_prestage.po_fact_update WHERE dw.po_fact.po_id = dw_prestage.po_fact_update.transaction_id AND   dw.po_fact.po_line_id = dw_prestage.po_fact_update.transaction_line_id);

/* fact -> NOW INSERT THE FACT RECORDS WHICH HAVE BEEN UPDATED AT THE SOURCE */ 
INSERT INTO dw.po_fact
(
  PO_NUMBER,
  PO_ID ,                  /* ADDD */  
  PO_LINE_ID  ,            /* ADDD */  
  REF_TRX_NUMBER ,         /* ADDD */  
  REF_TRX_TYPE ,           /* ADDD */  
  VENDOR_KEY  ,
  REQUESTER_KEY  ,
  APPROVER_LEVEL1_KEY  ,
  APPROVER_LEVEL2_KEY  ,
  CREATE_DATE_KEY  ,
  SUBSIDIARY_KEY  ,
  LOCATION_KEY  ,
  COST_CENTER_KEY  ,
  ITEM_KEY  ,
  REQUESTED_RECEIPT_DATE_KEY  ,
  ACTUAL_DELIVERY_DATE_KEY  ,
  FREIGHT_ESTIMATE_METHOD_KEY  ,
  TERMS_KEY  ,
  TAX_ITEM_KEY  ,
  CURRENCY_KEY  ,
  CLASS_KEY ,
  CARRIER_KEY,               /* ADDD */
  EXCHANGE_RATE,
  BILL_ADDRESS_LINE_1 ,        /* ADDD */
  BILL_ADDRESS_LINE_2 ,        /* ADDD */
  BILL_ADDRESS_LINE_3 ,        /* ADDD */
  BILL_CITY           ,        /* ADDD */
  BILL_COUNTRY        ,        /* ADDD */
  BILL_STATE          ,        /* ADDD */
  BILL_ZIP            ,        /* ADDD */
  SHIP_ADDRESS_LINE_1 ,        /* ADDD */
  SHIP_ADDRESS_LINE_2 ,        /* ADDD */
  SHIP_ADDRESS_LINE_3 ,        /* ADDD */
  SHIP_CITY           ,        /* ADDD */
  SHIP_COUNTRY        ,        /* ADDD */
  SHIP_STATE          ,        /* ADDD */
  SHIP_ZIP            ,        /* ADDD */
  QUANTITY,
  BIH_QUANTITY,
  BC_QUANTITY,
  TRADE_QUANTITY,
  NZSO_QUANTITY,
  EDUCATION_QUANTITY,
  SCHOOL_ESSENTIALS_QUANTITY,
  BOOK_FAIR_QUANTITY,  
  NUMBER_BILLED   ,                  /* ADDD */ 
  QUANTITY_RECEIVED_IN_SHIPMENT   ,  /* ADDD */ 
  QUANTITY_RETURNED  ,               /* ADDD */ 
  RATE,
  AMOUNT,
  ITEM_GROSS_AMOUNT,
  AMOUNT_FOREIGN,
  NET_AMOUNT,
  NET_AMOUNT_FOREIGN,
  GROSS_AMOUNT,
  MATCH_BILL_TO_RECEIPT,
  TRACK_LANDED_COST,
  TAX_AMOUNT,
  FREIGHT_RATE,
  PO_TYPE,
  STATUS,
  APPROVAL_STATUS,
  CREATION_DATE,
  LAST_MODIFIED_DATE,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_CURRENT
)
SELECT A.po_number AS po_number, 
       A.transaction_id AS po_id,            /* added */
       A.transaction_line_id AS po_line_id,  /* added */
       A.ref_trx_number AS ref_trx_number,   /* added */
       A.ref_trx_type AS ref_trx_type,       /* added */
       B.VENDOR_KEY AS VENDOR_KEY,
       C.EMPLOYEE_KEY AS REQUESTER_KEY,
       D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
       D1.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
       F.DATE_KEY AS CREATE_DATE_KEY,
       G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
       L.LOCATION_KEY AS LOCATION_KEY,
       R.DEPARTMENT_KEY AS COST_CENTER_KEY,
       M.ITEM_KEY AS ITEM_KEY,
       H.DATE_KEY AS REQUESTED_RECEIPT_DATE_KEY,
       I.DATE_KEY AS ACTUAL_DELIVERY_DATE_KEY,
       J.FREIGHT_KEY AS FREIGHT_ESTIMATE_METHOD_KEY,
       P.PAYMENT_TERM_KEY AS TERMS_KEY,
       Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
       K.CURRENCY_KEY AS CURRENCY_KEY,
       S.CLASS_KEY AS CLASS_KEY,
       T.CARRIER_KEY AS CARRIER_KEY,        /* added */
       A.EXCHANGE_RATE AS EXCHANGE_RATE,
       A.BILL_ADDRESS_LINE_1 ,         /* added */
       A.BILL_ADDRESS_LINE_2 ,         /* added */
       A.BILL_ADDRESS_LINE_3 ,         /* added */
       A.BILL_CITY           ,         /* added */
       A.BILL_COUNTRY        ,          /* added */
       A.BILL_STATE          ,         /* added */
       A.BILL_ZIP            ,          /* added */
       A.SHIP_ADDRESS_LINE_1 ,         /* added */
       A.SHIP_ADDRESS_LINE_2 ,         /* added */
       A.SHIP_ADDRESS_LINE_3 ,         /* added */
       A.SHIP_CITY           ,         /* added */
       A.SHIP_COUNTRY        ,         /* added */
       A.SHIP_STATE          ,         /* added */
       A.SHIP_ZIP            ,         /* added */
       A.ITEM_COUNT AS QUANTITY,
       A.BIH_QUANTITY AS BIH_QUANTITY,
       A.BC_QUANTITY AS BC_QUANTITY,
       A.TRADE_QUANTITY AS TRADE_QUANTITY,
       A.NZSO_QUANTITY AS NZSO_QUANTITY,
       A.EDUCATION_QUANTITY AS EDUCATION_QUANTITY,
       A.SCHOOL_ESSENTIALS_QUANTITY AS SCHOOL_ESSENTIALS_QUANTITY,
       A.BOOK_FAIR_QUANTITY AS BOOK_FAIR_QUANTITY,
       A.NUMBER_BILLED  ,                  /* added */
       A.QUANTITY_RECEIVED_IN_SHIPMENT  ,  /* added */
       A.QUANTITY_RETURNED       ,         /* added */
       A.ITEM_UNIT_PRICE AS RATE,
       A.AMOUNT AS AMOUNT,
       A.ITEM_GROSS_AMOUNT AS ITEM_GROSS_AMOUNT,
       A.AMOUNT_FOREIGN AS AMOUNT_FOREIGN,
       A.NET_AMOUNT AS NET_AMOUNT,
       A.NET_AMOUNT_FOREIGN AS NET_AMOUNT_FOREIGN,
       A.GROSS_AMOUNT AS GROSS_AMOUNT,
       A.MATCH_BILL_TO_RECEIPT as MATCH_BILL_TO_RECEIPT,
       A.TRACK_LANDED_COST as TRACK_LANDED_COST,
       A.TAX_AMOUNT AS TAX_AMOUNT,
       A.FRIGHT_RATE AS FREIGHT_RATE,
       A.PO_TYPE AS PO_TYPE,
       A.PO_STATUS AS STATUS,
       A.APPROVAL_STATUS AS APPROVAL_STATUS,
       A.CREATE_DATE AS CREATION_DATE,
       A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
       SYSDATE AS DATE_ACTIVE_FROM,
       '9999-12-31 11:59:59' AS DATE_ACTIVE_TO,
       1 AS DW_CURRENT
FROM dw_prestage.po_fact A
  INNER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,-99) = B.VENDOR_ID)  
  INNER JOIN DW.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,-99) = C.EMPLOYEE_ID)  
  INNER JOIN DW.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,-99) = D.EMPLOYEE_ID)  
  INNER JOIN DW.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,-99) = D1.EMPLOYEE_ID)  
  INNER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)  
  INNER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,-99) = G.SUBSIDIARY_ID)  
  INNER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.EXPECTED_RECEIPT_DATE,'YYYYMMDD'),'0') = H.DATE_ID)  
  INNER JOIN DW.DWDATE I ON (NVL (TO_CHAR (A.ACTUAL_DELIVERY_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  INNER JOIN DW.FREIGHT_ESTIMATE J ON (NVL (A.FREIGHT_ESTIMATE_METHOD_ID,-99) = J.LANDED_COST_RULE_MATRIX_NZ_ID)  
  INNER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,-99) = K.CURRENCY_ID)
  INNER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,-99) = L.LOCATION_ID)
  INNER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,-99) = M.ITEM_ID)
  INNER JOIN DW.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,-99) = P.PAYMENT_TERMS_ID)
  INNER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,-99) = Q.ITEM_ID)
  INNER JOIN DW.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,-99) = R.DEPARTMENT_ID)
  INNER JOIN DW.CLASSES S ON (NVL(A.CLASS_ID,-99) = S.CLASS_ID)
  INNER JOIN DW.CARRIER T ON (NVL(A.CARRIER_ID,-99) = T.CARRIER_ID)
  WHERE A.LINE_TYPE = 'PO_LINE'
AND   EXISTS (SELECT 1 FROM dw_prestage.po_fact_update WHERE a.transaction_id = dw_prestage.po_fact_update.transaction_id AND   a.transaction_line_id = dw_prestage.po_fact_update.transaction_line_id);

/* fact -> INSERT UPDATED RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.po_fact_error
(
RUNID,
PO_NUMBER,
PO_ID ,                  /* ADDD */  
PO_LINE_ID  ,            /* ADDD */  
REF_TRX_NUMBER ,         /* ADDD */  
REF_TRX_TYPE ,           /* ADDD */  
VENDOR_KEY,
REQUESTER_KEY,
APPROVER_LEVEL1_KEY,
APPROVER_LEVEL2_KEY,
CREATE_DATE_KEY,
SUBSIDIARY_KEY,
LOCATION_KEY,
COST_CENTER_KEY,
EXCHANGE_RATE,
ITEM_KEY,
BILL_ADDRESS_LINE_1 ,        /* ADDD */ 
BILL_ADDRESS_LINE_2 ,        /* ADDD */ 
BILL_ADDRESS_LINE_3 ,        /* ADDD */ 
BILL_CITY           ,        /* ADDD */ 
BILL_COUNTRY        ,        /* ADDD */ 
BILL_STATE          ,        /* ADDD */ 
BILL_ZIP            ,        /* ADDD */ 
SHIP_ADDRESS_LINE_1 ,        /* ADDD */ 
SHIP_ADDRESS_LINE_2 ,        /* ADDD */ 
SHIP_ADDRESS_LINE_3 ,        /* ADDD */ 
SHIP_CITY           ,        /* ADDD */ 
SHIP_COUNTRY        ,        /* ADDD */ 
SHIP_STATE          ,        /* ADDD */ 
SHIP_ZIP            ,        /* ADDD */ 
QUANTITY,
BIH_QUANTITY,
BC_QUANTITY,
TRADE_QUANTITY,
NZSO_QUANTITY,
EDUCATION_QUANTITY,
SCHOOL_ESSENTIALS_QUANTITY,
BOOK_FAIR_QUANTITY,
NUMBER_BILLED   ,                  /* ADDD */ 
QUANTITY_RECEIVED_IN_SHIPMENT   ,  /* ADDD */ 
QUANTITY_RETURNED  ,               /* ADDD */ 
RATE,
AMOUNT,
ITEM_GROSS_AMOUNT,
AMOUNT_FOREIGN,
NET_AMOUNT,
NET_AMOUNT_FOREIGN,
GROSS_AMOUNT,
MATCH_BILL_TO_RECEIPT,
TRACK_LANDED_COST,
TAX_ITEM_KEY,
TAX_AMOUNT,
REQUESTED_RECEIPT_DATE_KEY,
ACTUAL_DELIVERY_DATE_KEY,
FREIGHT_ESTIMATE_METHOD_KEY,
FREIGHT_RATE,
TERMS_KEY,
PO_TYPE,
STATUS,
APPROVAL_STATUS,
CREATION_DATE,
LAST_MODIFIED_DATE,
CURRENCY_KEY,
CLASS_KEY,  
CARRIER_KEY,               /* ADDD */ 
CARRIER_ID ,               /* ADDD */ 
VENDOR_ID,
REQUESTER_ID,
APPROVER_LEVEL1_ID,
APPROVER_LEVEL2_ID,
SUBSIDIARY_ID,
LOCATION_ID,
COST_CENTER_ID,
ITEM_ID,
TAX_ITEM_ID,
REQUESTED_RECEIPT_DATE,
ACTUAL_DELIVERY_DATE,
FREIGHT_ESTIMATE_METHOD_ID,
TERMS_ID,
CURRENCY_ID,
CLASS_ID,
RECORD_STATUS,
DW_CREATION_DATE
)
SELECT A.RUNID ,
A.PO_NUMBER,
A.transaction_id AS po_id,            /* added */
A.transaction_line_id AS po_line_id,  /* added */
A.ref_trx_number AS ref_trx_number,   /* added */
A.ref_trx_type AS ref_trx_type,       /* added */
B.VENDOR_KEY,
C.EMPLOYEE_KEY AS REQUESTER_KEY,
D.EMPLOYEE_KEY AS APPROVER_LEVEL1_KEY,
D.EMPLOYEE_KEY AS APPROVER_LEVEL2_KEY,
F.DATE_KEY AS CREATE_DATE_KEY,
G.SUBSIDIARY_KEY AS SUBSIDIARY_KEY,
L.LOCATION_KEY AS LOCATION_KEY,
R.DEPARTMENT_KEY AS COST_CENTER_KEY,
A.EXCHANGE_RATE,
M.ITEM_KEY AS ITEM_KEY,
A.BILL_ADDRESS_LINE_1 ,         /* added */  
A.BILL_ADDRESS_LINE_2 ,         /* added */  
A.BILL_ADDRESS_LINE_3 ,         /* added */  
A.BILL_CITY           ,         /* added */  
A.BILL_COUNTRY        ,          /* added */ 
A.BILL_STATE          ,         /* added */  
A.BILL_ZIP            ,          /* added */ 
A.SHIP_ADDRESS_LINE_1 ,         /* added */  
A.SHIP_ADDRESS_LINE_2 ,         /* added */  
A.SHIP_ADDRESS_LINE_3 ,         /* added */  
A.SHIP_CITY           ,         /* added */  
A.SHIP_COUNTRY        ,         /* added */  
A.SHIP_STATE          ,         /* added */  
A.SHIP_ZIP            ,         /* added */  
A.ITEM_COUNT AS QUANTITY,
A.BIH_QUANTITY,
A.BC_QUANTITY,
A.TRADE_QUANTITY,
A.NZSO_QUANTITY,
A.EDUCATION_QUANTITY,
A.SCHOOL_ESSENTIALS_QUANTITY,
A.BOOK_FAIR_QUANTITY,
A.NUMBER_BILLED  ,                  /* added */
A.QUANTITY_RECEIVED_IN_SHIPMENT  ,  /* added */
A.QUANTITY_RETURNED       ,         /* added */
A.ITEM_UNIT_PRICE AS RATE,
A.AMOUNT,
A.ITEM_GROSS_AMOUNT,
A.AMOUNT_FOREIGN,
A.NET_AMOUNT,
A.NET_AMOUNT_FOREIGN,
A.GROSS_AMOUNT,
A.MATCH_BILL_TO_RECEIPT,
A.TRACK_LANDED_COST,
Q.TAX_ITEM_KEY AS TAX_ITEM_KEY,
A.TAX_AMOUNT,
H.DATE_KEY AS REQUESTED_RECEIPT_DATE_KEY,
I.DATE_KEY AS ACTUAL_DELIVERY_DATE_KEY,
J.FREIGHT_KEY AS FREIGHT_ESTIMATE_METHOD_KEY,
A.FRIGHT_RATE AS FREIGHT_RATE,
P.PAYMENT_TERM_KEY AS TERMS_KEY,
A.PO_TYPE,
A.PO_STATUS AS STATUS,
A.APPROVAL_STATUS,
A.CREATE_DATE AS CREATION_DATE,
A.DATE_LAST_MODIFIED AS LAST_MODIFIED_DATE,
K.CURRENCY_KEY AS CURRENCY_KEY,
S.CLASS_KEY AS CLASS_KEY ,
T.CARRIER_KEY AS CARRIER_KEY,        /* added */
A.CARRIER_ID ,                        /* added */
A.VENDOR_ID ,
A.REQUESTOR_ID ,
A.APPROVER_LEVEL_ONE_ID ,
A.APPROVER_LEVEL_TWO_ID ,
A.SUBSIDIARY_ID ,
A.LOCATION_ID ,
A.DEPARTMENT_ID AS COST_CENTER_ID,
A.ITEM_ID,
A.TAX_ITEM_ID,
A.EXPECTED_RECEIPT_DATE,
A.ACTUAL_DELIVERY_DATE,
A.FREIGHT_ESTIMATE_METHOD_ID,
A.PAYMENT_TERMS_ID AS TERMS_ID,
A.CURRENCY_ID,
A.CLASS_ID,
'ERROR' AS RECORD_STATUS,
SYSDATE AS DW_CREATION_DATE
FROM dw_prestage.po_fact_insert A
  LEFT OUTER JOIN DW.VENDORS B ON (NVL (A.VENDOR_ID,0) = B.VENDOR_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES C ON (NVL (A.REQUESTOR_ID,0) = C.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES D ON (NVL (A.APPROVER_LEVEL_ONE_ID,0) = D.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.EMPLOYEES D1 ON (NVL (A.APPROVER_LEVEL_TWO_ID,0) = D1.EMPLOYEE_ID)  
  LEFT OUTER JOIN DW.DWDATE F ON (NVL (TO_CHAR (A.CREATE_DATE,'YYYYMMDD'),'0') = F.DATE_ID)  
  LEFT OUTER JOIN DW.SUBSIDIARIES G ON (NVL (A.SUBSIDIARY_ID,0) = G.SUBSIDIARY_ID)  
  LEFT OUTER JOIN DW.DWDATE H ON (NVL (TO_CHAR (A.EXPECTED_RECEIPT_DATE,'YYYYMMDD'),'0') = H.DATE_ID)  
  LEFT OUTER JOIN DW.DWDATE I ON (NVL (TO_CHAR (A.ACTUAL_DELIVERY_DATE,'YYYYMMDD'),'0') = I.DATE_ID)  
  LEFT OUTER JOIN DW.FREIGHT_ESTIMATE J ON (NVL (A.FREIGHT_ESTIMATE_METHOD_ID,0) = J.LANDED_COST_RULE_MATRIX_NZ_ID)  
  LEFT OUTER JOIN DW.CURRENCIES K ON (NVL (A.CURRENCY_ID,0) = K.CURRENCY_ID)
  LEFT OUTER JOIN DW.LOCATIONS L ON (NVL (A.LOCATION_ID,0) = L.LOCATION_ID)
  LEFT OUTER JOIN DW.ITEMS M ON (NVL (A.ITEM_ID,0) = M.ITEM_ID)
  LEFT OUTER JOIN DW.PAYMENT_TERMS P ON (NVL (A.PAYMENT_TERMS_ID,0) = P.PAYMENT_TERMS_ID)
  LEFT OUTER JOIN DW.TAX_ITEMS Q ON (NVL (A.TAX_ITEM_ID,0) = Q.ITEM_ID)
  LEFT OUTER JOIN DW.COST_CENTER R ON (NVL(A.DEPARTMENT_ID,0) = R.DEPARTMENT_ID)
  LEFT OUTER JOIN DW.CLASSES S ON (NVL(A.CLASS_ID,0) = S.CLASS_ID)
  LEFT OUTER JOIN DW.CARRIER T ON (NVL(A.CARRIER_ID,-99) = T.CARRIER_ID)
  WHERE A.LINE_TYPE = 'PO_LINE' AND 
  (B.VENDOR_KEY IS NULL OR
  C.EMPLOYEE_KEY IS NULL OR
  D.EMPLOYEE_KEY IS NULL OR
  D1.EMPLOYEE_KEY IS NULL OR
  F.DATE_KEY IS NULL OR
  G.SUBSIDIARY_KEY IS NULL OR
  L.LOCATION_KEY IS NULL OR
  R.DEPARTMENT_KEY IS NULL OR
  M.ITEM_KEY IS NULL OR
  Q.TAX_ITEM_KEY IS NULL OR
  H.DATE_KEY IS NULL OR
  I.DATE_KEY IS NULL OR
  J.FREIGHT_KEY IS NULL OR
  P.PAYMENT_TERM_KEY IS NULL OR
  K.CURRENCY_KEY IS NULL OR
  S.CLASS_KEY IS NULL OR
  T.CARRIER_KEY IS NULL )
AND   EXISTS (SELECT 1 FROM dw_prestage.po_fact_update WHERE a.transaction_id = dw_prestage.po_fact_update.transaction_id AND   a.transaction_line_id = dw_prestage.po_fact_update.transaction_line_id);