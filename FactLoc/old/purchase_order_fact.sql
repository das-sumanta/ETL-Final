SELECT
       TO_CHAR(TRANSACTIONS.TRANSACTION_ID) AS TRANSACTION_ID,
       REPLACE(REPLACE(TRANSACTIONS.TRANSACTION_NUMBER,CHR (10),' '),CHR (13),' ') AS PO_NUMBER,
       TO_CHAR(TRANSACTION_LINES.TRANSACTION_LINE_ID) AS TRANSACTION_LINE_ID,
       F.TRANSACTION_NUMBER AS REF_TRX_NUMBER,  
       F.TRANSACTION_TYPE AS REF_TRX_TYPE,      
       TO_CHAR(TRANSACTIONS.ENTITY_ID) AS VENDOR_ID,
       TO_CHAR(TRANSACTIONS.APPROVER_LEVEL_ONE_ID) AS APPROVER_LEVEL_ONE_ID,
       TO_CHAR(TRANSACTIONS.APPROVER_LEVEL_TWO_ID) AS APPROVER_LEVEL_TWO_ID,
       TRANSACTIONS.AMOUNT_UNBILLED,
       REPLACE(REPLACE(TRANSACTIONS.APPROVAL_STATUS,CHR (10),' '),CHR (13),' ') AS APPROVAL_STATUS,
       e.bill_address_line_1,
       e.bill_address_line_2,
       e.bill_address_line_3,
       e.bill_city,
       e.bill_country,
       e.bill_state,
       e.bill_zip,
       TO_CHAR(TRANSACTIONS.CARRIER_LEBEL_ID) AS CARRIER_ID,  
       TO_CHAR(TRANSACTIONS.CLOSED,'YYYY-MM-DD HH24:MI:SS') AS CLOSED,
       TO_CHAR(TRANSACTIONS.CREATED_BY_ID) AS CREATED_BY_ID,
       TO_CHAR(TRANSACTIONS.SALES_REP_ID) AS REQUESTOR_ID,
       TO_CHAR(TRANSACTIONS.CREATED_FROM_ID) AS CREATED_FROM_ID,
       TO_CHAR(TRANSACTIONS.CREATE_DATE,'YYYY-MM-DD HH24:MI:SS') AS CREATE_DATE,
       TO_CHAR(TRANSACTIONS.CURRENCY_ID) AS CURRENCY_ID,
       TO_CHAR(TRANSACTIONS.CUSTOM_FORM_ID) AS CUSTOM_FORM_ID,
       TO_CHAR(TRANSACTIONS.DATE_LAST_MODIFIED,'YYYY-MM-DD HH24:MI:SS') AS DATE_LAST_MODIFIED,
       TO_CHAR(TRANSACTIONS.EMPLOYEE_CUSTOM_ID) AS EMPLOYEE_CUSTOM_ID,
       TRANSACTIONS.EXCHANGE_RATE,
       TO_CHAR(TRANSACTIONS.LOCATION_ID) AS LOCATION_ID,
       TO_CHAR(TRANSACTIONS.PO_APPROVER_ID) AS PO_APPROVER_ID,
       e.ship_address_line_1,
       e.ship_address_line_2,
       e.ship_address_line_3,
       e.ship_city,
       e.ship_country,
       e.ship_state,
       e.ship_zip,
       TO_CHAR(TRANSACTION_LINES.SHIPMENT_RECEIVED,'YYYY-MM-DD HH24:MI:SS') AS SHIPMENT_RECEIVED,
       REPLACE(REPLACE(TRANSACTIONS.STATUS,CHR (10),' '),CHR (13),' ') AS PO_STATUS,
       TO_CHAR(TRANSACTIONS.PAYMENT_TERMS_ID) AS PAYMENT_TERMS_ID,
       TRANSACTION_LINES.FRIGHT_RATE,
       DECODE(CUSTOM_FORM_ID,
             199,'INTL Inventory Purchase Order',
             201,'INTL Drop Ship Purchase Order',
             202,'INTL Non-Inventory Purchase Order'
       ) AS PO_TYPE,
       TO_CHAR(TRANSACTION_LINES.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
       TO_CHAR(TRANSACTION_LINES.DEPARTMENT_ID) AS DEPARTMENT_ID,
       TO_CHAR(TRANSACTION_LINES.ITEM_ID) AS ITEM_ID,
       TO_CHAR(TRANSACTION_LINES.BC_QUANTITY) AS BC_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.BIH_QUANTITY) AS BIH_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.BOOK_FAIR_QUANTITY) AS BOOK_FAIR_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.EDUCATION_QUANTITY) AS EDUCATION_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.NZSO_QUANTITY) AS NZSO_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.TRADE_QUANTITY) AS TRADE_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.SCHOOL_ESSENTIALS_QUANTITY) AS SCHOOL_ESSENTIALS_QUANTITY,
       TO_CHAR(TRANSACTION_LINES.ITEM_COUNT) AS ITEM_COUNT,
       TO_CHAR(TRANSACTION_LINES.ITEM_GROSS_AMOUNT) AS ITEM_GROSS_AMOUNT,
       TO_CHAR(TRANSACTION_LINES.AMOUNT) AS AMOUNT,
       TO_CHAR(TRANSACTION_LINES.AMOUNT_FOREIGN) AS AMOUNT_FOREIGN,
       TO_CHAR(TRANSACTION_LINES.NET_AMOUNT) AS NET_AMOUNT,
       TO_CHAR(TRANSACTION_LINES.NET_AMOUNT_FOREIGN) AS NET_AMOUNT_FOREIGN,
       TO_CHAR(TRANSACTION_LINES.GROSS_AMOUNT) AS GROSS_AMOUNT,
       TO_CHAR(TRANSACTION_LINES.MATCH_BILL_TO_RECEIPT) AS MATCH_BILL_TO_RECEIPT,
       TO_CHAR(TRANSACTION_LINES.TRACK_LANDED_COST) AS TRACK_LANDED_COST,
       TO_CHAR(TRANSACTION_LINES.ITEM_UNIT_PRICE) AS ITEM_UNIT_PRICE,
       TO_CHAR(TRANSACTION_LINES.NUMBER_BILLED) AS NUMBER_BILLED,
       TO_CHAR(TRANSACTION_LINES.QUANTITY_RECEIVED_IN_SHIPMENT ) AS QUANTITY_RECEIVED_IN_SHIPMENT ,
       TO_CHAR(TRANSACTION_LINES.QUANTITY_RETURNED ) AS QUANTITY_RETURNED ,
       TO_CHAR(TRANSACTION_LINES.EXPECTED_RECEIPT_DATE,'YYYY-MM-DD HH24:MI:SS') AS EXPECTED_RECEIPT_DATE,
       TO_CHAR(TRANSACTION_LINES.ACTUAL_DELIVERY_DATE,'YYYY-MM-DD HH24:MI:SS') AS ACTUAL_DELIVERY_DATE,
       TO_CHAR(TRANSACTION_LINES.TAX_ITEM_ID) AS TAX_ITEM_ID,
       TRANSACTION_LINES.TAX_TYPE,
       C.AMOUNT_FOREIGN AS TAX_AMOUNT,
       TO_CHAR(TRANSACTION_LINES.FREIGHT_ESTIMATE_METHOD_ID) AS FREIGHT_ESTIMATE_METHOD_ID,
       Decode(b.name,
             'Purchase Orders','PO_HDR',
             decode(c.transaction_line_id,NULL,'PO_TAX','PO_LINE')
       )  AS line_type,
       TO_CHAR(TRANSACTION_LINES.CLASS_ID) as CLASS_ID
FROM TRANSACTION_LINES
  LEFT OUTER JOIN accounts b ON (TRANSACTION_LINES.account_id = accounts.account_id)
  LEFT OUTER JOIN transaction_tax_detail c
               ON (TRANSACTION_LINES.transaction_id = transaction_tax_detail.transaction_id
              AND TRANSACTION_LINES.transaction_line_id = transaction_tax_detail.transaction_line_id)
  LEFT OUTER JOIN transaction_address e ON (TRANSACTION_LINES.transaction_id = transaction_address.transaction_id)
  INNER JOIN transactions d ON (TRANSACTION_LINES.transaction_id = transactions.transaction_id)
  LEFT OUTER JOIN transactions f ON (d.created_from_id = f.transaction_id)
WHERE transactions.transaction_type = 'Purchase Order'
AND   (TRANSACTION_LINES.DATE_LAST_MODIFIED >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS') OR TRANSACTIONS.DATE_LAST_MODIFIED >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS'))
AND   transaction_lines.subsidiary_id = 27
ORDER BY TRANSACTION_LINES.transaction_id,
         TRANSACTION_LINES.transaction_line_id