/* prestage - drop intermediate insert table */ 
DROP TABLE if exists dw_prestage.revenue_fact_insert;

/* prestage - create intermediate insert table*/ 
CREATE TABLE dw_prestage.revenue_fact_insert 
AS
SELECT *
FROM dw_prestage.revenue_fact
WHERE EXISTS (SELECT 1
              FROM (SELECT TRANSACTION_ID,
                           transaction_line_id
                    FROM (SELECT TRANSACTION_ID,
                                 transaction_line_id
                          FROM dw_prestage.revenue_fact
                          MINUS
                          SELECT TRANSACTION_ID,
                                 transaction_line_id
                          FROM dw_stage.revenue_fact)) a
              WHERE dw_prestage.revenue_fact.TRANSACTION_ID = a.TRANSACTION_ID
              AND   dw_prestage.revenue_fact.transaction_line_id = a.transaction_line_id);

/* prestage - drop intermediate update table*/ 
DROP TABLE if exists dw_prestage.revenue_fact_update;

/* prestage - create intermediate update table*/ 
CREATE TABLE dw_prestage.revenue_fact_update 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM (SELECT TRANSACTION_NUMBER,
                   TRANSACTION_ID,
                   TRANSACTION_LINE_ID,
                   TRANSACTION_ORDER,
                   REF_DOC_NUMBER,
                   REF_CUSTOM_FORM_ID,
                   PAYMENT_TERMS_ID,
                   REVENUE_COMMITMENT_STATUS,
                   REVENUE_STATUS,
                   SALES_REP_ID,
                   BILL_ADDRESS_LINE_1,
                   BILL_ADDRESS_LINE_2,
                   BILL_ADDRESS_LINE_3,
                   BILL_CITY,
                   BILL_COUNTRY,
                   BILL_STATE,
                   BILL_ZIP,
                   SHIP_ADDRESS_LINE_1,
                   SHIP_ADDRESS_LINE_2,
                   SHIP_ADDRESS_LINE_3,
                   SHIP_CITY,
                   SHIP_COUNTRY,
                   SHIP_STATE,
                   SHIP_ZIP,
                   STATUS,
                   TRANSACTION_TYPE,
                   CURRENCY_ID,
                   TRANDATE,
                   EXCHANGE_RATE,
                   ACCOUNT_ID,
                   AMOUNT,
                   AMOUNT_FOREIGN,
                   GROSS_AMOUNT,
                   NET_AMOUNT,
                   NET_AMOUNT_FOREIGN,
                   QUANTITY,
                   ITEM_ID,
                   ITEM_UNIT_PRICE,
                   TAX_ITEM_ID,
                   TAX_AMOUNT,
                   LOCATION_ID,
                   CLASS_ID,
                   SUBSIDIARY_ID,
                   ACCOUNTING_PERIOD_ID,
                   CUSTOMER_ID,
                   PRICE_TYPE_ID,
                   CUSTOM_FORM_ID,
                   CREATED_BY_ID,
                   CREATE_DATE,
                   DATE_LAST_MODIFIED
            FROM dw_prestage.revenue_fact
            MINUS
            SELECT TRANSACTION_NUMBER,
                   TRANSACTION_ID,
                   TRANSACTION_LINE_ID,
                   TRANSACTION_ORDER,
                   REF_DOC_NUMBER,
                   REF_CUSTOM_FORM_ID,
                   PAYMENT_TERMS_ID,
                   REVENUE_COMMITMENT_STATUS,
                   REVENUE_STATUS,
                   SALES_REP_ID,
                   BILL_ADDRESS_LINE_1,
                   BILL_ADDRESS_LINE_2,
                   BILL_ADDRESS_LINE_3,
                   BILL_CITY,
                   BILL_COUNTRY,
                   BILL_STATE,
                   BILL_ZIP,
                   SHIP_ADDRESS_LINE_1,
                   SHIP_ADDRESS_LINE_2,
                   SHIP_ADDRESS_LINE_3,
                   SHIP_CITY,
                   SHIP_COUNTRY,
                   SHIP_STATE,
                   SHIP_ZIP,
                   STATUS,
                   TRANSACTION_TYPE,
                   CURRENCY_ID,
                   TRANDATE,
                   EXCHANGE_RATE,
                   ACCOUNT_ID,
                   AMOUNT,
                   AMOUNT_FOREIGN,
                   GROSS_AMOUNT,
                   NET_AMOUNT,
                   NET_AMOUNT_FOREIGN,
                   QUANTITY,
                   ITEM_ID,
                   ITEM_UNIT_PRICE,
                   TAX_ITEM_ID,
                   TAX_AMOUNT,
                   LOCATION_ID,
                   CLASS_ID,
                   SUBSIDIARY_ID,
                   ACCOUNTING_PERIOD_ID,
                   CUSTOMER_ID,
                   PRICE_TYPE_ID,
                   CUSTOM_FORM_ID,
                   CREATED_BY_ID,
                   CREATE_DATE,
                   DATE_LAST_MODIFIED
            FROM dw_stage.revenue_fact)) a
WHERE NOT EXISTS (SELECT 1
                  FROM dw_prestage.revenue_fact_insert
                  WHERE dw_prestage.revenue_fact_insert.TRANSACTION_ID = a.TRANSACTION_ID
                  AND   dw_prestage.revenue_fact_insert.transaction_line_id = a.transaction_line_id);

/* prestage - drop intermediate no change track table*/ 
DROP TABLE if exists dw_prestage.revenue_fact_nochange;

/* prestage - create intermediate no change track table*/ 
CREATE TABLE dw_prestage.revenue_fact_nochange 
AS
SELECT TRANSACTION_ID,
       transaction_line_id
FROM (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.revenue_fact
      MINUS
      (SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.revenue_fact_insert
      UNION ALL
      SELECT TRANSACTION_ID,
             transaction_line_id
      FROM dw_prestage.revenue_fact_update));

/* prestage-> stage*/ 
SELECT 'no of revenue fact records ingested in staging -->' ||count(1)
FROM dw_prestage.revenue_fact;

/* prestage-> stage*/ 
SELECT 'no of revenue fact records identified to inserted -->' ||count(1)
FROM dw_prestage.revenue_fact_insert;

/* prestage-> stage*/ 
SELECT 'no of revenue fact records identified to updated -->' ||count(1)
FROM dw_prestage.revenue_fact_update;

/* prestage-> stage*/ 
SELECT 'no of revenue fact records identified as no change -->' ||count(1)
FROM dw_prestage.revenue_fact_nochange;

--D --A = B + C + D
/* stage -> delete from stage records to be updated */ 
DELETE
FROM dw_stage.revenue_fact USING dw_prestage.revenue_fact_update
WHERE dw_stage.revenue_fact.transaction_id = dw_prestage.revenue_fact_update.transaction_id
AND   dw_stage.revenue_fact.transaction_line_id = dw_prestage.revenue_fact_update.transaction_line_id;

/* stage -> insert into stage records which have been created */ 
INSERT INTO dw_stage.revenue_fact(runid
 ,transaction_number
 ,transaction_id
 ,transaction_line_id
 ,transaction_order
 ,ref_doc_number
 ,REF_CUSTOM_FORM_ID
 ,payment_terms_id
 ,revenue_commitment_status
 ,revenue_status
 ,sales_rep_id
 ,bill_address_line_1
 ,bill_address_line_2
 ,bill_address_line_3
 ,bill_city
 ,bill_country
 ,bill_state
 ,bill_zip
 ,ship_address_line_1
 ,ship_address_line_2
 ,ship_address_line_3
 ,ship_city
 ,ship_country
 ,ship_state
 ,ship_zip
 ,status
 ,transaction_type
 ,currency_id
 ,trandate
 ,exchange_rate
 ,account_id
 ,amount
 ,amount_foreign
 ,gross_amount
 ,net_amount
 ,net_amount_foreign
 ,quantity
 ,item_id
 ,item_unit_price
 ,tax_item_id
 ,tax_amount
 ,location_id
 ,class_id
 ,subsidiary_id
 ,accounting_period_id
 ,customer_id
 ,trx_type
 ,custom_form_id
 ,created_by_id
 ,create_date
 ,price_type_id
 ,date_last_modified)
SELECT runid
 ,transaction_number
 ,transaction_id
 ,transaction_line_id
 ,transaction_order
 ,ref_doc_number
 ,REF_CUSTOM_FORM_ID
 ,payment_terms_id
 ,revenue_commitment_status
 ,revenue_status
 ,sales_rep_id
 ,bill_address_line_1
 ,bill_address_line_2
 ,bill_address_line_3
 ,bill_city
 ,bill_country
 ,bill_state
 ,bill_zip
 ,ship_address_line_1
 ,ship_address_line_2
 ,ship_address_line_3
 ,ship_city
 ,ship_country
 ,ship_state
 ,ship_zip
 ,status
 ,transaction_type
 ,currency_id
 ,trandate
 ,exchange_rate
 ,account_id
 ,amount
 ,amount_foreign
 ,gross_amount
 ,net_amount
 ,net_amount_foreign
 ,quantity
 ,item_id
 ,item_unit_price
 ,tax_item_id
 ,tax_amount
 ,location_id
 ,class_id
 ,subsidiary_id
 ,accounting_period_id
 ,customer_id
 ,trx_type
 ,custom_form_id
 ,created_by_id
 ,create_date
 ,price_type_id
 ,date_last_modified
FROM dw_prestage.revenue_fact_insert;

/* stage -> insert into stage records which have been updated */ 
INSERT INTO dw_stage.revenue_fact
(runid
 ,transaction_number
 ,transaction_id
 ,transaction_line_id
 ,transaction_order
 ,ref_doc_number
 ,REF_CUSTOM_FORM_ID
 ,payment_terms_id
 ,revenue_commitment_status
 ,revenue_status
 ,sales_rep_id
 ,bill_address_line_1
 ,bill_address_line_2
 ,bill_address_line_3
 ,bill_city
 ,bill_country
 ,bill_state
 ,bill_zip
 ,ship_address_line_1
 ,ship_address_line_2
 ,ship_address_line_3
 ,ship_city
 ,ship_country
 ,ship_state
 ,ship_zip
 ,status
 ,transaction_type
 ,currency_id
 ,trandate
 ,exchange_rate
 ,account_id
 ,amount
 ,amount_foreign
 ,gross_amount
 ,net_amount
 ,net_amount_foreign
 ,quantity
 ,item_id
 ,item_unit_price
 ,tax_item_id
 ,tax_amount
 ,location_id
 ,class_id
 ,subsidiary_id
 ,accounting_period_id
 ,customer_id
 ,trx_type
 ,custom_form_id
 ,created_by_id
 ,create_date
 ,price_type_id
 ,date_last_modified)
SELECT runid
 ,transaction_number
 ,transaction_id
 ,transaction_line_id
 ,transaction_order
 ,ref_doc_number
 ,REF_CUSTOM_FORM_ID
 ,payment_terms_id
 ,revenue_commitment_status
 ,revenue_status
 ,sales_rep_id
 ,bill_address_line_1
 ,bill_address_line_2
 ,bill_address_line_3
 ,bill_city
 ,bill_country
 ,bill_state
 ,bill_zip
 ,ship_address_line_1
 ,ship_address_line_2
 ,ship_address_line_3
 ,ship_city
 ,ship_country
 ,ship_state
 ,ship_zip
 ,status
 ,transaction_type
 ,currency_id
 ,trandate
 ,exchange_rate
 ,account_id
 ,amount
 ,amount_foreign
 ,gross_amount
 ,net_amount
 ,net_amount_foreign
 ,quantity
 ,item_id
 ,item_unit_price
 ,tax_item_id
 ,tax_amount
 ,location_id
 ,class_id
 ,subsidiary_id
 ,accounting_period_id
 ,customer_id
 ,trx_type
 ,custom_form_id
 ,created_by_id
 ,create_date
 ,price_type_id
 ,date_last_modified
FROM dw_prestage.revenue_fact
WHERE EXISTS (SELECT 1
              FROM dw_prestage.revenue_fact_update
              WHERE dw_prestage.revenue_fact_update.transaction_id = dw_prestage.revenue_fact.transaction_id
              AND   dw_prestage.revenue_fact_update.transaction_line_id = dw_prestage.revenue_fact.transaction_line_id);

COMMIT;

/* fact -> INSERT NEW RECORDS WHICH HAS ALL VALID DIMENSIONS */ 
insert into dw.revenue_fact
(
DOCUMENT_NUMBER            
,TRANSACTION_ID             
,TRANSACTION_LINE_ID        
,REF_DOC_NUMBER             
,REF_DOC_TYPE_KEY           
,TERMS_KEY                  
,REVENUE_COMMITMENT_STATUS  
,REVENUE_STATUS             
,TERRITORY_KEY              
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,DOCUMENT_STATUS_KEY        
,DOCUMENT_TYPE_KEY          
,CURRENCY_KEY               
,TRANSACTION_DATE_KEY       
,EXCHANGE_RATE              
,ACCOUNT_KEY                
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
,ITEM_KEY                   
,RATE                       
,TAX_ITEM_KEY               
,TAX_AMOUNT                 
,LOCATION_KEY               
,CLASS_KEY                  
,SUBSIDIARY_KEY             
,CUSTOMER_KEY               
,ACCOUNTING_PERIOD_KEY
,DATE_ACTIVE_FROM           
,DATE_ACTIVE_TO             
,DW_CURRENT           
)
select 
TRANSACTION_NUMBER        
,TRANSACTION_ID            
,TRANSACTION_LINE_ID       
,REF_DOC_NUMBER            
,n.transaction_type_key AS REF_DOC_TYPE_KEY              
,b.PAYMENT_TERM_KEY AS TERMS_KEY
,REVENUE_COMMITMENT_STATUS 
,REVENUE_STATUS 
,c.territory_key
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,o.transaction_status_key  as DOCUMENT_STATUS_KEY  
,p.transaction_type_key    as DOCUMENT_TYPE_KEY
,d.currency_key
,e.date_key as TRANSACTION_DATE_KEY       
,EXCHANGE_RATE             
,f.account_key
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
 ,g.item_key
 ,ITEM_UNIT_PRICE        as rate
 ,h.TAX_ITEM_KEY
 ,TAX_AMOUNT                 
 ,k.location_key
 ,l.class_key 
 ,j.subsidiary_key
 ,m.customer_key
 ,q.accounting_period_key
 ,SYSDATE AS DATE_ACTIVE_FROM
 ,'9999-12-31 11:59:59' AS DATE_ACTIVE_TO
 ,1 AS DW_CURRENT
 from dw_prestage.revenue_fact a 
 INNER JOIN DW_REPORT.PAYMENT_TERMS b ON (NVL (A.PAYMENT_TERMS_ID,-99) = b.PAYMENT_TERMS_ID)
 INNER JOIN DW_REPORT.territories c ON (NVL (A.sales_rep_ID,-99) = c.territory_ID)
 INNER JOIN DW_REPORT.CURRENCIES d ON (NVL (A.CURRENCY_ID,-99) = d.CURRENCY_ID)
 INNER JOIN DW_REPORT.DWDATE e ON (NVL (TO_CHAR (A.tranDATE,'YYYYMMDD'),'0') = e.DATE_ID)
 INNER JOIN DW_REPORT.ACCOUNTS F ON (NVL (A.account_ID,-99) = f.account_ID)
 INNER JOIN DW_REPORT.ITEMS g ON (NVL (A.ITEM_ID,-99) = g.ITEM_ID)
 INNER JOIN DW_REPORT.TAX_ITEMS h ON (NVL (A.TAX_ITEM_ID,-99) = h.ITEM_ID)
 INNER JOIN DW_REPORT.SUBSIDIARIES j ON (NVL (A.SUBSIDIARY_ID,-99) = j.SUBSIDIARY_ID)  
 INNER JOIN DW_REPORT.LOCATIONS k ON (NVL (A.LOCATION_ID,-99) = k.LOCATION_ID)
 INNER JOIN DW_REPORT.CLASSES l ON (NVL(A.CLASS_ID,-99) = l.CLASS_ID) 
 INNER JOIN DW_REPORT.customers m ON (NVL(A.customer_ID,-99) = m.customer_ID) 
 INNER JOIN DW_REPORT.transaction_type n ON (NVL(A.ref_custom_form_id,-99) = n.transaction_type_id) 
 INNER JOIN DW_REPORT.transaction_status o ON (NVL(A.STATUS,'NA_GDW') = o.status AND NVL(A.TRANSACTION_TYPE,'NA_GDW') = o.DOCUMENT_TYPE)
 INNER JOIN DW_REPORT.transaction_type p ON (NVL(A.custom_form_id,-99) = p.transaction_type_id) 
 INNER JOIN DW_REPORT.accounting_period q ON (NVL(A.accounting_period_id,-99) = q.accounting_period_id)   
where trx_type in ('INV_LINE','RA_LINE','CN_LINE','JN_LINE' );
  
/* fact -> INSERT NEW RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.revenue_fact_error
(
   RUNID                                 
  ,DOCUMENT_NUMBER             
  ,TRANSACTION_ID              
  ,TRANSACTION_LINE_ID         
  ,REF_DOC_NUMBER              
  ,REF_DOC_TYPE_KEY            
  ,TERMS_KEY                   
  ,REVENUE_COMMITMENT_STATUS   
  ,REVENUE_STATUS              
  ,TERRITORY_KEY               
  ,BILL_ADDRESS_LINE_1         
  ,BILL_ADDRESS_LINE_2         
  ,BILL_ADDRESS_LINE_3         
  ,BILL_CITY                   
  ,BILL_COUNTRY                
  ,BILL_STATE                  
  ,BILL_ZIP                    
  ,SHIP_ADDRESS_LINE_1         
  ,SHIP_ADDRESS_LINE_2         
  ,SHIP_ADDRESS_LINE_3         
  ,SHIP_CITY                   
  ,SHIP_COUNTRY                
  ,SHIP_STATE                  
  ,SHIP_ZIP                    
  ,DOCUMENT_STATUS_KEY         
  ,DOCUMENT_TYPE_KEY           
  ,CURRENCY_KEY                
  ,TRANSACTION_DATE_KEY        
  ,EXCHANGE_RATE               
  ,ACCOUNT_KEY                 
  ,AMOUNT                      
  ,AMOUNT_FOREIGN              
  ,GROSS_AMOUNT                
  ,NET_AMOUNT                  
  ,NET_AMOUNT_FOREIGN          
  ,QUANTITY                    
  ,ITEM_KEY                    
  ,RATE                        
  ,TAX_ITEM_KEY                
  ,TAX_AMOUNT                  
  ,LOCATION_KEY                
  ,CLASS_KEY                   
  ,SUBSIDIARY_KEY              
  ,CUSTOMER_KEY                
  ,ACCOUNTING_PERIOD_KEY       
  ,REF_CUSTOM_FORM_ID          
  ,CUSTOM_FORM_ID              
  ,PAYMENT_TERMS_ID            
  ,SALES_REP_ID                
  ,STATUS                      
  ,CURRENCY_ID                 
  ,TRANDATE                    
  ,ACCOUNT_ID                  
  ,ITEM_ID                     
  ,TAX_ITEM_ID                 
  ,LOCATION_ID                 
  ,CLASS_ID                    
  ,SUBSIDIARY_ID               
  ,CUSTOMER_ID                 
  ,ACCOUNTING_PERIOD_ID        
  ,RECORD_STATUS               
  ,DW_CREATION_DATE            
)
SELECT 
  A.RUNID 
, A.TRANSACTION_NUMBER        
,A.TRANSACTION_ID            
,A.TRANSACTION_LINE_ID       
,A.REF_DOC_NUMBER            
,n.transaction_type_key AS REF_DOC_TYPE_KEY              
,b.PAYMENT_TERM_KEY AS TERMS_KEY
,REVENUE_COMMITMENT_STATUS 
,REVENUE_STATUS 
,c.territory_key
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,o.transaction_status_key  as DOCUMENT_STATUS_KEY  
,p.transaction_type_key    as DOCUMENT_TYPE_KEY
,d.currency_key
,e.date_key as TRANSACTION_DATE_KEY       
,EXCHANGE_RATE             
,f.account_key
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
 ,g.item_key
 ,ITEM_UNIT_PRICE        as rate
 ,h.TAX_ITEM_KEY
 ,TAX_AMOUNT                 
 ,k.location_key
 ,l.class_key 
 ,j.subsidiary_key
 ,m.customer_key
 ,q.accounting_period_key
 ,A.ref_custom_form_id
 ,A.custom_form_id
 ,A.PAYMENT_TERMS_ID
 ,A.sales_rep_ID
 ,A.STATUS
,A.CURRENCY_ID
,A.TRANDATE
 , A.ACCOUNT_ID
, A.ITEM_ID
, A.TAX_ITEM_ID
,A.LOCATION_ID
,A.CLASS_ID
,A.SUBSIDIARY_ID
,A.CUSTOMER_ID
,A.ACCOUNTING_PERIOD_ID
,'ERROR' AS RECORD_STATUS
,SYSDATE AS DW_CREATION_DATE
 from dw_prestage.revenue_fact a 
 LEFT OUTER JOIN DW_REPORT.PAYMENT_TERMS b ON (NVL (A.PAYMENT_TERMS_ID,-99) = b.PAYMENT_TERMS_ID)
 LEFT OUTER JOIN DW_REPORT.territories c ON (NVL (A.sales_rep_ID,-99) = c.territory_ID)
 LEFT OUTER JOIN DW_REPORT.CURRENCIES d ON (NVL (A.CURRENCY_ID,-99) = d.CURRENCY_ID)
 LEFT OUTER JOIN DW_REPORT.DWDATE e ON (NVL (TO_CHAR (A.tranDATE,'YYYYMMDD'),'0') = e.DATE_ID)
 LEFT OUTER JOIN DW_REPORT.ACCOUNTS F ON (NVL (A.account_ID,-99) = f.account_ID)
 LEFT OUTER JOIN DW_REPORT.ITEMS g ON (NVL (A.ITEM_ID,-99) = g.ITEM_ID)
 LEFT OUTER JOIN DW_REPORT.TAX_ITEMS h ON (NVL (A.TAX_ITEM_ID,-99) = h.ITEM_ID)
 LEFT OUTER JOIN DW_REPORT.SUBSIDIARIES j ON (NVL (A.SUBSIDIARY_ID,-99) = j.SUBSIDIARY_ID)  
 LEFT OUTER JOIN DW_REPORT.LOCATIONS k ON (NVL (A.LOCATION_ID,-99) = k.LOCATION_ID)
 LEFT OUTER JOIN DW_REPORT.CLASSES l ON (NVL(A.CLASS_ID,-99) = l.CLASS_ID) 
 LEFT OUTER JOIN DW_REPORT.customers m ON (NVL(A.customer_ID,-99) = m.customer_ID) 
 LEFT OUTER JOIN DW_REPORT.transaction_type n ON (NVL(A.ref_custom_form_id,-99) = n.transaction_type_id) 
 LEFT OUTER JOIN DW_REPORT.transaction_status o ON (NVL(A.STATUS,'NA_GDW') = o.status AND NVL(A.TRANSACTION_TYPE,'NA_GDW') = o.DOCUMENT_TYPE)
 LEFT OUTER JOIN DW_REPORT.transaction_type p ON (NVL(A.custom_form_id,-99) = p.transaction_type_id) 
 LEFT OUTER JOIN DW_REPORT.accounting_period q ON (NVL(A.accounting_period_id,-99) = q.accounting_period_id)   
where trx_type in ('INV_LINE','RA_LINE','CN_LINE','JN_LINE' ) AND
(B.PAYMENT_TERM_KEY IS NULL OR
 C.TERRITORY_KEY IS NULL OR
 D.CURRENCY_KEY IS NULL OR
 E.DATE_KEY IS NULL OR
 F.ACCOUNT_KEY IS NULL OR
 G.ITEM_KEY IS NULL OR
 H.TAX_ITEM_KEY IS NULL OR
 J.SUBSIDIARY_KEY IS NULL OR
 K.LOCATION_KEY IS NULL OR
 L.CLASS_KEY IS NULL OR
 M.CUSTOMER_KEY IS NULL OR
 N.transaction_type_key IS NULL OR
 O.transaction_status_key IS NULL OR
 P.transaction_type_key IS NULL OR
 Q.ACCOUNTING_PERIOD_KEY IS NULL);
 

/* fact -> UPDATE THE OLD RECORDS SETTING THE CURRENT FLAG VALUE TO 0 */  
UPDATE dw.revenue_fact SET dw_current = 0,DATE_ACTIVE_TO = (sysdate -1) WHERE dw_current = 1
AND   sysdate>= date_active_from
AND   sysdate< date_active_to
AND   EXISTS (SELECT 1 FROM dw_prestage.revenue_fact_update 
  WHERE dw.revenue_fact.transaction_ID = dw_prestage.revenue_fact_update.transaction_id 
  AND   dw.revenue_fact.transaction_LINE_ID = dw_prestage.revenue_fact_update.transaction_line_id);

/* fact -> NOW INSERT THE FACT RECORDS WHICH HAVE BEEN UPDATED AT THE SOURCE */ 
insert into dw.revenue_fact
(
DOCUMENT_NUMBER            
,TRANSACTION_ID             
,TRANSACTION_LINE_ID        
,REF_DOC_NUMBER             
,REF_DOC_TYPE_KEY           
,TERMS_KEY                  
,REVENUE_COMMITMENT_STATUS  
,REVENUE_STATUS             
,TERRITORY_KEY              
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,DOCUMENT_STATUS_KEY        
,DOCUMENT_TYPE_KEY          
,CURRENCY_KEY               
,TRANSACTION_DATE_KEY       
,EXCHANGE_RATE              
,ACCOUNT_KEY                
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
,ITEM_KEY                   
,RATE                       
,TAX_ITEM_KEY               
,TAX_AMOUNT                 
,LOCATION_KEY               
,CLASS_KEY                  
,SUBSIDIARY_KEY             
,CUSTOMER_KEY               
,ACCOUNTING_PERIOD_KEY
,DATE_ACTIVE_FROM           
,DATE_ACTIVE_TO             
,DW_CURRENT           
)
select 
TRANSACTION_NUMBER        
,TRANSACTION_ID            
,TRANSACTION_LINE_ID       
,REF_DOC_NUMBER            
,n.transaction_type_key AS REF_DOC_TYPE_KEY              
,b.PAYMENT_TERM_KEY AS TERMS_KEY
,REVENUE_COMMITMENT_STATUS 
,REVENUE_STATUS 
,c.territory_key
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,o.transaction_status_key  as DOCUMENT_STATUS_KEY  
,p.transaction_type_key    as DOCUMENT_TYPE_KEY
,d.currency_key
,e.date_key as TRANSACTION_DATE_KEY       
,EXCHANGE_RATE             
,f.account_key
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
 ,g.item_key
 ,ITEM_UNIT_PRICE        as rate
 ,h.TAX_ITEM_KEY
 ,TAX_AMOUNT                 
 ,k.location_key
 ,l.class_key 
 ,j.subsidiary_key
 ,m.customer_key
 ,q.accounting_period_key
 ,SYSDATE AS DATE_ACTIVE_FROM
 ,'9999-12-31 11:59:59' AS DATE_ACTIVE_TO
 ,1 AS DW_CURRENT
 from dw_prestage.revenue_fact a 
 INNER JOIN DW_REPORT.PAYMENT_TERMS b ON (NVL (A.PAYMENT_TERMS_ID,-99) = b.PAYMENT_TERMS_ID)
 INNER JOIN DW_REPORT.territories c ON (NVL (A.sales_rep_ID,-99) = c.territory_ID)
 INNER JOIN DW_REPORT.CURRENCIES d ON (NVL (A.CURRENCY_ID,-99) = d.CURRENCY_ID)
 INNER JOIN DW_REPORT.DWDATE e ON (NVL (TO_CHAR (A.tranDATE,'YYYYMMDD'),'0') = e.DATE_ID)
 INNER JOIN DW_REPORT.ACCOUNTS F ON (NVL (A.account_ID,-99) = f.account_ID)
 INNER JOIN DW_REPORT.ITEMS g ON (NVL (A.ITEM_ID,-99) = g.ITEM_ID)
 INNER JOIN DW_REPORT.TAX_ITEMS h ON (NVL (A.TAX_ITEM_ID,-99) = h.ITEM_ID)
 INNER JOIN DW_REPORT.SUBSIDIARIES j ON (NVL (A.SUBSIDIARY_ID,-99) = j.SUBSIDIARY_ID)  
 INNER JOIN DW_REPORT.LOCATIONS k ON (NVL (A.LOCATION_ID,-99) = k.LOCATION_ID)
 INNER JOIN DW_REPORT.CLASSES l ON (NVL(A.CLASS_ID,-99) = l.CLASS_ID) 
 INNER JOIN DW_REPORT.customers m ON (NVL(A.customer_ID,-99) = m.customer_ID) 
 INNER JOIN DW_REPORT.transaction_type n ON (NVL(A.ref_custom_form_id,-99) = n.transaction_type_id) 
 INNER JOIN DW_REPORT.transaction_status o ON (NVL(A.STATUS,'NA_GDW') = o.status AND NVL(A.TRANSACTION_TYPE,'NA_GDW') = o.DOCUMENT_TYPE)
 INNER JOIN DW_REPORT.transaction_type p ON (NVL(A.custom_form_id,-99) = p.transaction_type_id) 
 INNER JOIN DW_REPORT.accounting_period q ON (NVL(A.accounting_period_id,-99) = q.accounting_period_id)   
where trx_type in ('INV_LINE','RA_LINE','CN_LINE','JN_LINE' )
AND   EXISTS (SELECT 1 FROM dw_prestage.revenue_fact_update 
 WHERE a.transaction_id = dw_prestage.revenue_fact_update.transaction_id 
 AND   a.transaction_line_id = dw_prestage.revenue_fact_update.transaction_line_id);

/* fact -> INSERT UPDATED RECORDS IN ERROR TABLE WHICH DOES NOT HAVE VALID DIMENSIONS */ 
INSERT INTO dw.revenue_fact_error
(
   RUNID                                 
  ,DOCUMENT_NUMBER             
  ,TRANSACTION_ID              
  ,TRANSACTION_LINE_ID         
  ,REF_DOC_NUMBER              
  ,REF_DOC_TYPE_KEY            
  ,TERMS_KEY                   
  ,REVENUE_COMMITMENT_STATUS   
  ,REVENUE_STATUS              
  ,TERRITORY_KEY               
  ,BILL_ADDRESS_LINE_1         
  ,BILL_ADDRESS_LINE_2         
  ,BILL_ADDRESS_LINE_3         
  ,BILL_CITY                   
  ,BILL_COUNTRY                
  ,BILL_STATE                  
  ,BILL_ZIP                    
  ,SHIP_ADDRESS_LINE_1         
  ,SHIP_ADDRESS_LINE_2         
  ,SHIP_ADDRESS_LINE_3         
  ,SHIP_CITY                   
  ,SHIP_COUNTRY                
  ,SHIP_STATE                  
  ,SHIP_ZIP                    
  ,DOCUMENT_STATUS_KEY         
  ,DOCUMENT_TYPE_KEY           
  ,CURRENCY_KEY                
  ,TRANSACTION_DATE_KEY        
  ,EXCHANGE_RATE               
  ,ACCOUNT_KEY                 
  ,AMOUNT                      
  ,AMOUNT_FOREIGN              
  ,GROSS_AMOUNT                
  ,NET_AMOUNT                  
  ,NET_AMOUNT_FOREIGN          
  ,QUANTITY                    
  ,ITEM_KEY                    
  ,RATE                        
  ,TAX_ITEM_KEY                
  ,TAX_AMOUNT                  
  ,LOCATION_KEY                
  ,CLASS_KEY                   
  ,SUBSIDIARY_KEY              
  ,CUSTOMER_KEY                
  ,ACCOUNTING_PERIOD_KEY       
  ,REF_CUSTOM_FORM_ID          
  ,CUSTOM_FORM_ID              
  ,PAYMENT_TERMS_ID            
  ,SALES_REP_ID                
  ,STATUS                      
  ,CURRENCY_ID                 
  ,TRANDATE                    
  ,ACCOUNT_ID                  
  ,ITEM_ID                     
  ,TAX_ITEM_ID                 
  ,LOCATION_ID                 
  ,CLASS_ID                    
  ,SUBSIDIARY_ID               
  ,CUSTOMER_ID                 
  ,ACCOUNTING_PERIOD_ID        
  ,RECORD_STATUS               
  ,DW_CREATION_DATE            
)
SELECT 
  A.RUNID 
, A.TRANSACTION_NUMBER        
,A.TRANSACTION_ID            
,A.TRANSACTION_LINE_ID       
,A.REF_DOC_NUMBER            
,n.transaction_type_key AS REF_DOC_TYPE_KEY              
,b.PAYMENT_TERM_KEY AS TERMS_KEY
,REVENUE_COMMITMENT_STATUS 
,REVENUE_STATUS 
,c.territory_key
,BILL_ADDRESS_LINE_1        
,BILL_ADDRESS_LINE_2        
,BILL_ADDRESS_LINE_3        
,BILL_CITY                  
,BILL_COUNTRY               
,BILL_STATE                 
,BILL_ZIP                   
,SHIP_ADDRESS_LINE_1        
,SHIP_ADDRESS_LINE_2        
,SHIP_ADDRESS_LINE_3        
,SHIP_CITY                  
,SHIP_COUNTRY               
,SHIP_STATE                 
,SHIP_ZIP                   
,o.transaction_status_key  as DOCUMENT_STATUS_KEY  
,p.transaction_type_key    as DOCUMENT_TYPE_KEY
,d.currency_key
,e.date_key as TRANSACTION_DATE_KEY       
,EXCHANGE_RATE             
,f.account_key
,AMOUNT                     
,AMOUNT_FOREIGN             
,GROSS_AMOUNT               
,NET_AMOUNT                 
,NET_AMOUNT_FOREIGN         
,QUANTITY                   
 ,g.item_key
 ,ITEM_UNIT_PRICE        as rate
 ,h.TAX_ITEM_KEY
 ,TAX_AMOUNT                 
 ,k.location_key
 ,l.class_key 
 ,j.subsidiary_key
 ,m.customer_key
 ,q.accounting_period_key
 ,A.ref_custom_form_id
 ,A.custom_form_id
 ,A.PAYMENT_TERMS_ID
 ,A.sales_rep_ID
 ,A.STATUS
,A.CURRENCY_ID
,A.TRANDATE
 , A.ACCOUNT_ID
, A.ITEM_ID
, A.TAX_ITEM_ID
,A.LOCATION_ID
,A.CLASS_ID
,A.SUBSIDIARY_ID
,A.CUSTOMER_ID
,A.ACCOUNTING_PERIOD_ID
,'ERROR' AS RECORD_STATUS
,SYSDATE AS DW_CREATION_DATE
 from dw_prestage.revenue_fact a 
 LEFT OUTER JOIN DW_REPORT.PAYMENT_TERMS b ON (NVL (A.PAYMENT_TERMS_ID,-99) = b.PAYMENT_TERMS_ID)
 LEFT OUTER JOIN DW_REPORT.territories c ON (NVL (A.sales_rep_ID,-99) = c.territory_ID)
 LEFT OUTER JOIN DW_REPORT.CURRENCIES d ON (NVL (A.CURRENCY_ID,-99) = d.CURRENCY_ID)
 LEFT OUTER JOIN DW_REPORT.DWDATE e ON (NVL (TO_CHAR (A.tranDATE,'YYYYMMDD'),'0') = e.DATE_ID)
 LEFT OUTER JOIN DW_REPORT.ACCOUNTS F ON (NVL (A.account_ID,-99) = f.account_ID)
 LEFT OUTER JOIN DW_REPORT.ITEMS g ON (NVL (A.ITEM_ID,-99) = g.ITEM_ID)
 LEFT OUTER JOIN DW_REPORT.TAX_ITEMS h ON (NVL (A.TAX_ITEM_ID,-99) = h.ITEM_ID)
 LEFT OUTER JOIN DW_REPORT.SUBSIDIARIES j ON (NVL (A.SUBSIDIARY_ID,-99) = j.SUBSIDIARY_ID)  
 LEFT OUTER JOIN DW_REPORT.LOCATIONS k ON (NVL (A.LOCATION_ID,-99) = k.LOCATION_ID)
 LEFT OUTER JOIN DW_REPORT.CLASSES l ON (NVL(A.CLASS_ID,-99) = l.CLASS_ID) 
 LEFT OUTER JOIN DW_REPORT.customers m ON (NVL(A.customer_ID,-99) = m.customer_ID) 
 LEFT OUTER JOIN DW_REPORT.transaction_type n ON (NVL(A.ref_custom_form_id,-99) = n.transaction_type_id) 
 LEFT OUTER JOIN DW_REPORT.transaction_status o ON (NVL(A.STATUS,'NA_GDW') = o.status AND NVL(A.TRANSACTION_TYPE,'NA_GDW') = o.DOCUMENT_TYPE)
 LEFT OUTER JOIN DW_REPORT.transaction_type p ON (NVL(A.custom_form_id,-99) = p.transaction_type_id) 
 LEFT OUTER JOIN DW_REPORT.accounting_period q ON (NVL(A.accounting_period_id,-99) = q.accounting_period_id)   
where trx_type in ('INV_LINE','RA_LINE','CN_LINE','JN_LINE' ) AND
(B.PAYMENT_TERM_KEY IS NULL OR
 C.TERRITORY_KEY IS NULL OR
 D.CURRENCY_KEY IS NULL OR
 E.DATE_KEY IS NULL OR
 F.ACCOUNT_KEY IS NULL OR
 G.ITEM_KEY IS NULL OR
 H.TAX_ITEM_KEY IS NULL OR
 J.SUBSIDIARY_KEY IS NULL OR
 K.LOCATION_KEY IS NULL OR
 L.CLASS_KEY IS NULL OR
 M.CUSTOMER_KEY IS NULL OR
 N.transaction_type_key IS NULL OR
 O.transaction_status_key IS NULL OR
 P.transaction_type_key IS NULL OR
 Q.ACCOUNTING_PERIOD_KEY IS NULL)
AND   EXISTS (SELECT 1 
             FROM dw_prestage.revenue_fact_update 
             WHERE 
                   a.transaction_id = dw_prestage.revenue_fact_update.transaction_id 
             AND   a.transaction_line_id = dw_prestage.revenue_fact_update.transaction_line_id);
             
COMMIT;