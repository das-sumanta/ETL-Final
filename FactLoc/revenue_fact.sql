SELECT b.transaction_number AS document_number,
       to_char(a.transaction_id) as transaction_id,
       to_char(a.transaction_line_id) as transaction_line_id,
       to_char(a.transaction_order) as transaction_order,
       f.transaction_number AS REF_DOC_NUMBER,
       f.custom_form_id as ref_custom_form_id,
       TO_CHAR(b.payment_terms_id) AS payment_terms_id,
       b.revenue_commitment_status,
       b.revenue_status,
       TO_CHAR(h.sales_rep_id) as sales_rep_id,
       e.BILL_ADDRESS_LINE_1,
       e.BILL_ADDRESS_LINE_2,
       e.BILL_ADDRESS_LINE_3,
       e.BILL_CITY,
       e.BILL_COUNTRY,
       e.BILL_STATE,
       e.BILL_ZIP,
       e.SHIP_ADDRESS_LINE_1,
       e.SHIP_ADDRESS_LINE_2,
       e.SHIP_ADDRESS_LINE_3,
       e.SHIP_CITY,
       e.SHIP_COUNTRY,
       e.SHIP_STATE,
       e.SHIP_ZIP,
       status AS document_status,
       b.transaction_type AS transaction_type,
       TO_CHAR(b.currency_id) as currency_id,
       TO_CHAR(b.trandate,'YYYY-MM-DD HH24:MI:SS') AS trandate,
       b.EXCHANGE_RATE,
       to_char(a.account_id) as account_id,
       to_char(a.AMOUNT) as AMOUNT,
       to_char(a.AMOUNT_FOREIGN) as AMOUNT_FOREIGN,
       to_char(a.GROSS_AMOUNT) as GROSS_AMOUNT,
       to_char(a.NET_AMOUNT) as NET_AMOUNT,
       to_char(a.NET_AMOUNT_FOREIGN) as NET_AMOUNT_FOREIGN,
       to_char(a.item_count) as quantity,
       to_char(a.item_id) as item_id,
       TO_CHAR(a.ITEM_UNIT_PRICE) AS ITEM_UNIT_PRICE,
       TO_CHAR(a.TAX_ITEM_ID) AS TAX_ITEM_ID,
       TO_CHAR(d.AMOUNT) AS TAX_AMOUNT,
       TO_CHAR(b.LOCATION_ID) AS LOCATION_ID,
       TO_CHAR(a.CLASS_ID) as CLASS_ID,
       TO_CHAR(a.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
       TO_CHAR(B.accounting_period_ID) AS accounting_period_ID,
       TO_CHAR(B.entity_ID) AS customer_ID,
       TO_CHAR(a.price_type_ID) AS price_type_ID,
       TO_CHAR(B.custom_form_ID) AS custom_form_ID,
       TO_CHAR(b.created_by_ID) AS created_by_ID,
       TO_CHAR(B.create_date,'YYYY-MM-DD HH24:MI:SS') AS create_date,
       TO_CHAR(a.date_last_modified,'YYYY-MM-DD HH24:MI:SS') AS date_last_modified,
       Decode(c.name,
             'AR-General','INV_HDR',
             decode(d.transaction_line_id,NULL,'INV_TAX',DECODE(c.name,'Revenue-Product','INV_LINE','INV_FRT'))
       )  AS line_type 
FROM transaction_lines a
  INNER JOIN transactions b ON (TRANSACTION_LINES.transaction_id = transactions.transaction_id)
  LEFT OUTER JOIN accounts c ON (TRANSACTION_LINES.account_id = accounts.account_id)
  LEFT OUTER JOIN transaction_tax_detail d
               ON (TRANSACTION_LINES.transaction_id = transaction_tax_detail.transaction_id
              AND TRANSACTION_LINES.transaction_line_id = transaction_tax_detail.transaction_line_id)
  LEFT OUTER JOIN transaction_address e ON (TRANSACTION_LINES.transaction_id = transaction_address.transaction_id)
  LEFT OUTER JOIN transactions f ON (b.created_from_id = f.transaction_id)
  LEFT OUTER JOIN customers h ON (b.ENTITY_ID = h.customer_id)
WHERE a.subsidiary_id = 27
AND   b.transaction_type = 'Invoice'
AND   EXISTS (SELECT 1
              FROM transactions g
              WHERE g.transaction_id = b.created_from_id
              AND   g.transaction_type = 'Sales Order')
AND a.date_last_modified >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')
UNION ALL
SELECT b.transaction_number AS document_number,
       to_char(a.transaction_id) as transaction_id,
       to_char(a.transaction_line_id) as transaction_line_id,
       to_char(a.transaction_order) as transaction_order,
       f.transaction_number AS REF_DOC_NUMBER,
       f.custom_form_id as ref_custom_form_id,
       TO_CHAR(b.payment_terms_id) AS payment_terms_id,
       b.revenue_commitment_status,
       b.revenue_status,
       TO_CHAR(h.sales_rep_id) as sales_rep_id,
       e.BILL_ADDRESS_LINE_1,
       e.BILL_ADDRESS_LINE_2,
       e.BILL_ADDRESS_LINE_3,
       e.BILL_CITY,
       e.BILL_COUNTRY,
       e.BILL_STATE,
       e.BILL_ZIP,
       e.SHIP_ADDRESS_LINE_1,
       e.SHIP_ADDRESS_LINE_2,
       e.SHIP_ADDRESS_LINE_3,
       e.SHIP_CITY,
       e.SHIP_COUNTRY,
       e.SHIP_STATE,
       e.SHIP_ZIP,
       status AS document_status,
       b.transaction_type AS transaction_type,
       TO_CHAR(b.currency_id) as currency_id,
       TO_CHAR(b.trandate,'YYYY-MM-DD HH24:MI:SS') AS trandate,
       b.EXCHANGE_RATE,
       to_char(a.account_id) as account_id,
       to_char(a.AMOUNT) as AMOUNT,
       to_char(a.AMOUNT_FOREIGN) as AMOUNT_FOREIGN,
       to_char(a.GROSS_AMOUNT) as GROSS_AMOUNT,
       to_char(a.NET_AMOUNT) as NET_AMOUNT,
       to_char(a.NET_AMOUNT_FOREIGN) as NET_AMOUNT_FOREIGN,
       to_char(a.item_count) as quantity,
       to_char(a.item_id) as item_id,
       TO_CHAR(a.ITEM_UNIT_PRICE) AS ITEM_UNIT_PRICE,
       TO_CHAR(a.TAX_ITEM_ID) AS TAX_ITEM_ID,
       TO_CHAR(d.AMOUNT) AS TAX_AMOUNT,
       TO_CHAR(b.LOCATION_ID) AS LOCATION_ID,
       TO_CHAR(a.CLASS_ID) as CLASS_ID,
       TO_CHAR(a.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
       TO_CHAR(B.accounting_period_ID) AS accounting_period_ID,
       TO_CHAR(B.entity_ID) AS customer_ID,
       TO_CHAR(a.price_type_ID) AS price_type_ID,
       TO_CHAR(B.custom_form_ID) AS custom_form_ID,
       TO_CHAR(b.created_by_ID) AS created_by_ID,
       TO_CHAR(B.create_date,'YYYY-MM-DD HH24:MI:SS') AS create_date,
       TO_CHAR(a.date_last_modified,'YYYY-MM-DD HH24:MI:SS') AS date_last_modified,
       Decode(c.name,
             'Return Authorizations','RA_HDR',
             NULL,'RA_OTH',
             decode(d.transaction_line_id,NULL,'RA_TAX',DECODE(c.name,'Revenue-Product','RA_LINE','RA_FRT'))
       )  AS line_type 
FROM transaction_lines a
  INNER JOIN transactions b ON (TRANSACTION_LINES.transaction_id = transactions.transaction_id)
  LEFT OUTER JOIN accounts c ON (TRANSACTION_LINES.account_id = accounts.account_id)
  LEFT OUTER JOIN transaction_tax_detail d
               ON (TRANSACTION_LINES.transaction_id = transaction_tax_detail.transaction_id
              AND TRANSACTION_LINES.transaction_line_id = transaction_tax_detail.transaction_line_id)
  LEFT OUTER JOIN transaction_address e ON (TRANSACTION_LINES.transaction_id = transaction_address.transaction_id)
  LEFT OUTER JOIN transactions f ON (b.created_from_id = f.transaction_id)
  LEFT OUTER JOIN customers h ON (b.ENTITY_ID = h.customer_id)
WHERE a.subsidiary_id = 27
AND   b.transaction_type = 'Return Authorization' 
AND EXISTS ( select 1 from transactions g
						 WHERE g.transaction_id = b.created_from_id
						 AND g.transaction_type IN ('Sales Order','Invoice') )
AND a.date_last_modified >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')
UNION ALL
SELECT b.transaction_number AS document_number,
       to_char(a.transaction_id) as transaction_id,
       to_char(a.transaction_line_id) as transaction_line_id,
       to_char(a.transaction_order) as transaction_order,
       f.transaction_number AS REF_DOC_NUMBER,
       f.custom_form_id as ref_custom_form_id,
       TO_CHAR(b.payment_terms_id) AS payment_terms_id,
       b.revenue_commitment_status,
       b.revenue_status,
       TO_CHAR(h.sales_rep_id) as sales_rep_id,
       e.BILL_ADDRESS_LINE_1,
       e.BILL_ADDRESS_LINE_2,
       e.BILL_ADDRESS_LINE_3,
       e.BILL_CITY,
       e.BILL_COUNTRY,
       e.BILL_STATE,
       e.BILL_ZIP,
       e.SHIP_ADDRESS_LINE_1,
       e.SHIP_ADDRESS_LINE_2,
       e.SHIP_ADDRESS_LINE_3,
       e.SHIP_CITY,
       e.SHIP_COUNTRY,
       e.SHIP_STATE,
       e.SHIP_ZIP,
       status AS document_status,
       b.transaction_type AS transaction_type,
       TO_CHAR(b.currency_id) as currency_id,
       TO_CHAR(b.trandate,'YYYY-MM-DD HH24:MI:SS') AS trandate,
       b.EXCHANGE_RATE,
       to_char(a.account_id) as account_id,
       to_char(a.AMOUNT) as AMOUNT,
       to_char(a.AMOUNT_FOREIGN) as AMOUNT_FOREIGN,
       to_char(a.GROSS_AMOUNT) as GROSS_AMOUNT,
       to_char(a.NET_AMOUNT) as NET_AMOUNT,
       to_char(a.NET_AMOUNT_FOREIGN) as NET_AMOUNT_FOREIGN,
       to_char(a.item_count) as quantity,
       to_char(a.item_id) as item_id,
       TO_CHAR(a.ITEM_UNIT_PRICE) AS ITEM_UNIT_PRICE,
       TO_CHAR(a.TAX_ITEM_ID) AS TAX_ITEM_ID,
       TO_CHAR(d.AMOUNT) AS TAX_AMOUNT,
       TO_CHAR(b.LOCATION_ID) AS LOCATION_ID,
       TO_CHAR(a.CLASS_ID) as CLASS_ID,
       TO_CHAR(a.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
       TO_CHAR(B.accounting_period_ID) AS accounting_period_ID,
       TO_CHAR(B.entity_ID) AS customer_ID,
       TO_CHAR(a.price_type_ID) AS price_type_ID,
       TO_CHAR(B.custom_form_ID) AS custom_form_ID,
       TO_CHAR(b.created_by_ID) AS created_by_ID,
       TO_CHAR(B.create_date,'YYYY-MM-DD HH24:MI:SS') AS create_date,
       TO_CHAR(a.date_last_modified,'YYYY-MM-DD HH24:MI:SS') AS date_last_modified,
       Decode(c.name,
             'AR-General','CN_HDR',
             NULL,'CN_OTH',
             decode(d.transaction_line_id,NULL,'CN_TAX',DECODE(c.name,'Revenue-Product','CN_LINE','CN_OTH'))
       )  AS line_type 
FROM transaction_lines a
  INNER JOIN transactions b ON (TRANSACTION_LINES.transaction_id = transactions.transaction_id)
  LEFT OUTER JOIN accounts c ON (TRANSACTION_LINES.account_id = accounts.account_id)
  LEFT OUTER JOIN transaction_tax_detail d
               ON (TRANSACTION_LINES.transaction_id = transaction_tax_detail.transaction_id
              AND TRANSACTION_LINES.transaction_line_id = transaction_tax_detail.transaction_line_id)
  LEFT OUTER JOIN transaction_address e ON (TRANSACTION_LINES.transaction_id = transaction_address.transaction_id)
  LEFT OUTER JOIN transactions f ON (b.created_from_id = f.transaction_id)
  LEFT OUTER JOIN customers h ON (b.ENTITY_ID = h.customer_id)
WHERE a.subsidiary_id = 27
AND   b.transaction_type = 'Credit Memo'
AND EXISTS ( select 1 from transactions g
						 WHERE g.transaction_id = b.created_from_id
						 AND g.transaction_type IN ('Sales Order','Invoice') )
AND a.date_last_modified >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')
UNION ALL
SELECT b.transaction_number AS document_number,
       to_char(a.transaction_id) as transaction_id,
       to_char(a.transaction_line_id) as transaction_line_id,
       to_char(a.transaction_order) as transaction_order,
       f.transaction_number AS REF_DOC_NUMBER,
       f.custom_form_id as ref_custom_form_id,
       TO_CHAR(b.payment_terms_id) AS payment_terms_id,
       b.revenue_commitment_status,
       b.revenue_status,
       TO_CHAR(h.sales_rep_id) as sales_rep_id,
       e.BILL_ADDRESS_LINE_1,
       e.BILL_ADDRESS_LINE_2,
       e.BILL_ADDRESS_LINE_3,
       e.BILL_CITY,
       e.BILL_COUNTRY,
       e.BILL_STATE,
       e.BILL_ZIP,
       e.SHIP_ADDRESS_LINE_1,
       e.SHIP_ADDRESS_LINE_2,
       e.SHIP_ADDRESS_LINE_3,
       e.SHIP_CITY,
       e.SHIP_COUNTRY,
       e.SHIP_STATE,
       e.SHIP_ZIP,
       status AS document_status,
       b.transaction_type AS transaction_type,
       TO_CHAR(b.currency_id) as currency_id,
       TO_CHAR(b.trandate,'YYYY-MM-DD HH24:MI:SS') AS trandate,
       b.EXCHANGE_RATE,
       to_char(a.account_id) as account_id,
       to_char(a.AMOUNT) as AMOUNT,
       to_char(a.AMOUNT_FOREIGN) as AMOUNT_FOREIGN,
       to_char(a.GROSS_AMOUNT) as GROSS_AMOUNT,
       to_char(a.NET_AMOUNT) as NET_AMOUNT,
       to_char(a.NET_AMOUNT_FOREIGN) as NET_AMOUNT_FOREIGN,
       to_char(a.item_count) as quantity,
       to_char(a.item_id) as item_id,
       TO_CHAR(a.ITEM_UNIT_PRICE) AS ITEM_UNIT_PRICE,
       TO_CHAR(a.TAX_ITEM_ID) AS TAX_ITEM_ID,
       TO_CHAR(d.AMOUNT) AS TAX_AMOUNT,
       TO_CHAR(b.LOCATION_ID) AS LOCATION_ID,
       TO_CHAR(a.CLASS_ID) as CLASS_ID,
       TO_CHAR(a.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
       TO_CHAR(B.accounting_period_ID) AS accounting_period_ID,
       TO_CHAR(B.entity_ID) AS customer_ID,
       TO_CHAR(a.price_type_ID) AS price_type_ID,
       TO_CHAR(B.custom_form_ID) AS custom_form_ID,
       TO_CHAR(b.created_by_ID) AS created_by_ID,
       TO_CHAR(B.create_date,'YYYY-MM-DD HH24:MI:SS') AS create_date,
       TO_CHAR(a.date_last_modified,'YYYY-MM-DD HH24:MI:SS') AS date_last_modified,
       Decode(c.name,
             'AR-General','JN_HDR',
             NULL,'JN_OTH',
             decode(d.transaction_line_id,NULL,'JN_TAX',DECODE(c.name,'Revenue-Product','JN_LINE','JN_OTH'))
       )  AS line_type 
FROM transaction_lines a
  INNER JOIN transactions b ON (TRANSACTION_LINES.transaction_id = transactions.transaction_id)
  LEFT OUTER JOIN accounts c ON (TRANSACTION_LINES.account_id = accounts.account_id)
  LEFT OUTER JOIN transaction_tax_detail d
               ON (TRANSACTION_LINES.transaction_id = transaction_tax_detail.transaction_id
              AND TRANSACTION_LINES.transaction_line_id = transaction_tax_detail.transaction_line_id)
  LEFT OUTER JOIN transaction_address e ON (TRANSACTION_LINES.transaction_id = transaction_address.transaction_id)
  LEFT OUTER JOIN transactions f ON (b.created_from_id = f.transaction_id)
  LEFT OUTER JOIN customers h ON (b.ENTITY_ID = h.customer_id)
WHERE a.subsidiary_id = 27
AND   b.transaction_type = 'Journal'
AND   c.accountnumber = '4000001'
AND c.full_name = 'Product Revenue : Revenue-Product'
AND a.date_last_modified >= to_timestamp('%s','YYYY-MM-DD HH24:MI:SS')