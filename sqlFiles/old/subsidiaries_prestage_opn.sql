/* prestage - drop intermediate insert table */
drop table if exists dw_prestage.subsidiaries_insert;

/* prestage - create intermediate insert table*/
create table dw_prestage.subsidiaries_insert
as
select * from dw_prestage.subsidiaries
where exists ( select 1 from  
(select subsidiary_id from 
(select subsidiary_id from dw_prestage.subsidiaries
minus
select subsidiary_id from dw_stage.subsidiaries )) a
where  dw_prestage.subsidiaries.subsidiary_id = a.subsidiary_id );

/* prestage - drop intermediate update table*/
drop table if exists dw_prestage.subsidiaries_update;

/* prestage - create intermediate update table*/
create table dw_prestage.subsidiaries_update
as
select decode(sum(ch_type),3,2,sum(ch_type)) ch_type ,subsidiary_id
from
(
SELECT subsidiary_id , CH_TYPE FROM 
(  
select subsidiary_id , FISCAL_CALENDAR_ID,CURRENCY,BASE_CURRENCY_ID,PARENT_ID, '2' CH_TYPE from dw_prestage.subsidiaries
MINUS
select subsidiary_id , FISCAL_CALENDAR_ID,CURRENCY,BASE_CURRENCY_ID,PARENT_ID, '2' CH_TYPE from dw_stage.subsidiaries
)
union all
SELECT subsidiary_id , CH_TYPE FROM 
(  
select subsidiary_id,NAME,ISINACTIVE,EDITION,IS_ELIMINATION,LEGAL_NAME,FEDERAL_NUMBER,LEGAL_ENTITY_ACCOUNT_CODE, '1' CH_TYPE from dw_prestage.subsidiaries
MINUS
select subsidiary_id,NAME,ISINACTIVE,EDITION,IS_ELIMINATION,LEGAL_NAME,FEDERAL_NUMBER,LEGAL_ENTITY_ACCOUNT_CODE, '1' CH_TYPE from dw_stage.subsidiaries
)
) a where not exists ( select 1 from dw_prestage.subsidiaries_insert
where dw_prestage.subsidiaries_insert.subsidiary_id = a.subsidiary_id) group by subsidiary_id;

/* prestage - drop intermediate delete table*/
drop table if exists dw_prestage.subsidiaries_delete;

/* prestage - create intermediate delete table*/
create table dw_prestage.subsidiaries_delete
as
select * from dw_stage.subsidiaries
where exists ( select 1 from  
(select subsidiary_id from 
(select subsidiary_id from dw_stage.subsidiaries
minus
select subsidiary_id from dw_prestage.subsidiaries )) a
where dw_stage.subsidiaries.subsidiary_id = a.subsidiary_id );

/* prestage-> stage*/
select 'no of prestage vendor records identified to inserted -->'||count(1) from  dw_prestage.subsidiaries_insert;

/* prestage-> stage*/
select 'no of prestage vendor records identified to updated -->'||count(1) from  dw_prestage.subsidiaries_update;

/* prestage-> stage*/
select 'no of prestage vendor records identified to deleted -->'||count(1) from  dw_prestage.subsidiaries_delete;

/* stage -> delete from stage records to be updated */
delete from dw_stage.subsidiaries 
using dw_prestage.subsidiaries_update
where dw_stage.subsidiaries.subsidiary_id = dw_prestage.subsidiaries_update.subsidiary_id;

/* stage -> delete from stage records which have been deleted */
delete from dw_stage.subsidiaries 
using dw_prestage.subsidiaries_delete
where dw_stage.subsidiaries.subsidiary_id = dw_prestage.subsidiaries_delete.subsidiary_id;

/* stage -> insert into stage records which have been created */
insert into dw_stage.subsidiaries (BASE_CURRENCY_ID,BRANCH_ID,BRN,DATE_LAST_MODIFIED,EDITION,FEDERAL_NUMBER ,FISCAL_CALENDAR_ID ,FULL_NAME ,ISINACTIVE ,IS_ELIMINATION ,IS_MOSS ,LEGAL_ENTITY_ACCOUNT_CODE ,LEGAL_NAME ,MOSS_NEXUS_ID ,NAME ,PARENT_ID ,PURCHASEORDERAMOUNT ,PURCHASEORDERQUANTITY ,PURCHASEORDERQUANTITYDIFF ,RECEIPTAMOUNT ,RECEIPTQUANTITY ,RECEIPTQUANTITYDIFF ,STATE_TAX_NUMBER ,SUBSIDIARY_EXTID ,SUBSIDIARY_ID ,TRAN_NUM_PREFIX ,UEN ,URL ,CURRENCY)
select BASE_CURRENCY_ID,BRANCH_ID,BRN,DATE_LAST_MODIFIED,EDITION,FEDERAL_NUMBER ,FISCAL_CALENDAR_ID ,FULL_NAME ,ISINACTIVE ,IS_ELIMINATION ,IS_MOSS ,LEGAL_ENTITY_ACCOUNT_CODE ,LEGAL_NAME ,MOSS_NEXUS_ID ,NAME ,PARENT_ID ,PURCHASEORDERAMOUNT ,PURCHASEORDERQUANTITY ,PURCHASEORDERQUANTITYDIFF ,RECEIPTAMOUNT ,RECEIPTQUANTITY ,RECEIPTQUANTITYDIFF ,STATE_TAX_NUMBER ,SUBSIDIARY_EXTID ,SUBSIDIARY_ID ,TRAN_NUM_PREFIX ,UEN ,URL ,CURRENCY from dw_prestage.subsidiaries_insert;

/* stage -> insert into stage records which have been created */
insert into dw_stage.subsidiaries (BASE_CURRENCY_ID,BRANCH_ID,BRN,DATE_LAST_MODIFIED,EDITION,FEDERAL_NUMBER ,FISCAL_CALENDAR_ID ,FULL_NAME ,ISINACTIVE ,IS_ELIMINATION ,IS_MOSS ,LEGAL_ENTITY_ACCOUNT_CODE ,LEGAL_NAME ,MOSS_NEXUS_ID ,NAME ,PARENT_ID ,PURCHASEORDERAMOUNT ,PURCHASEORDERQUANTITY ,PURCHASEORDERQUANTITYDIFF ,RECEIPTAMOUNT ,RECEIPTQUANTITY ,RECEIPTQUANTITYDIFF ,STATE_TAX_NUMBER ,SUBSIDIARY_EXTID ,SUBSIDIARY_ID ,TRAN_NUM_PREFIX ,UEN ,URL ,CURRENCY)
select BASE_CURRENCY_ID,BRANCH_ID,BRN,DATE_LAST_MODIFIED,EDITION,FEDERAL_NUMBER ,FISCAL_CALENDAR_ID ,FULL_NAME ,ISINACTIVE ,IS_ELIMINATION ,IS_MOSS ,LEGAL_ENTITY_ACCOUNT_CODE ,LEGAL_NAME ,MOSS_NEXUS_ID ,NAME ,PARENT_ID ,PURCHASEORDERAMOUNT ,PURCHASEORDERQUANTITY ,PURCHASEORDERQUANTITYDIFF ,RECEIPTAMOUNT ,RECEIPTQUANTITY ,RECEIPTQUANTITYDIFF ,STATE_TAX_NUMBER ,SUBSIDIARY_EXTID ,SUBSIDIARY_ID ,TRAN_NUM_PREFIX ,UEN ,URL ,CURRENCY from dw_prestage.subsidiaries
where exists ( select 1 from 
dw_prestage.subsidiaries_update
where dw_prestage.subsidiaries_update.subsidiary_id = dw_prestage.subsidiaries.subsidiary_id);

commit;

/* dimension ->insert new records in dim subsidiaries */

insert into dw.subsidiaries ( 
  SUBSIDIARY_ID,
  NAME,
  ISINACTIVE,
  EDITION,
  ELIMINATION,
  LEGAL_NAME,
  VAT_REG_NO,
  LEGAL_ACCOUNT_CODE,
  FISCAL_CALENDAR_ID,
  CURRENCY,
  CURRENCY_ID,
  PARENT_SUBSIDIARY,
  PARENT_SUBSIDIARY_ID,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE )
select 
 A.subsidiary_id
 ,DECODE(LENGTH(A.NAME),0,'NA_GDW',A.NAME)
 ,DECODE(LENGTH(A.ISINACTIVE),0,'NA_GDW',A.ISINACTIVE )         
 ,DECODE(LENGTH(A.EDITION),0,'NA_GDW',A.EDITION   )      
 ,DECODE(LENGTH(A.IS_ELIMINATION),0,'NA_GDW',A.IS_ELIMINATION   )    
 ,DECODE(LENGTH(A.LEGAL_NAME),0,'NA_GDW',A.LEGAL_NAME )    
 ,DECODE(LENGTH(A.FEDERAL_NUMBER),0,'NA_GDW',A.FEDERAL_NUMBER )    
 ,DECODE(LENGTH(A.LEGAL_ENTITY_ACCOUNT_CODE),0,'NA_GDW',A.LEGAL_ENTITY_ACCOUNT_CODE   )
 ,NVL(A.FISCAL_CALENDAR_ID,-99)         
 ,DECODE(LENGTH(A.CURRENCY),0,'NA_GDW', A.CURRENCY  )
 ,NVL(A.BASE_CURRENCY_ID,-99)       
 ,DECODE(LENGTH(B.NAME),0,'NA_GDW', B.NAME)          
 ,NVL(A.PARENT_ID,-99)       
 ,sysdate
 ,'9999-12-31 23:59:59'
 ,'A'
  from 
dw_prestage.subsidiaries_insert A
,dw_prestage.subsidiaries B
WHERE A.PARENT_ID = B.subsidiary_id(+);

/* dimension -> update old record as part of SCD2 maintenance*/

UPDATE dw.subsidiaries
   SET dw_active = 'I' ,
	   DATE_ACTIVE_TO = (sysdate -1)
WHERE dw_active = 'A'
      and sysdate >= date_active_from and sysdate < date_active_to
	  and exists ( select 1 from dw_prestage.subsidiaries_update
	  WHERE dw.subsidiaries.subsidiary_id = dw_prestage.subsidiaries_update.subsidiary_id
	  and dw_prestage.subsidiaries_update.ch_type = 2);

/* dimension -> insert the new records as part of SCD2 maintenance*/

insert into dw.subsidiaries ( 
   SUBSIDIARY_ID,
  NAME,
  ISINACTIVE,
  EDITION,
  ELIMINATION,
  LEGAL_NAME,
  VAT_REG_NO,
  LEGAL_ACCOUNT_CODE,
  FISCAL_CALENDAR_ID,
  CURRENCY,
  CURRENCY_ID,
  PARENT_SUBSIDIARY,
  PARENT_SUBSIDIARY_ID,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE )
select 
 A.subsidiary_id
 ,DECODE(LENGTH(A.NAME),0,'NA_GDW',A.NAME)
 ,DECODE(LENGTH(A.ISINACTIVE),0,'NA_GDW',A.ISINACTIVE )         
 ,DECODE(LENGTH(A.EDITION),0,'NA_GDW',A.EDITION   )      
 ,DECODE(LENGTH(A.IS_ELIMINATION),0,'NA_GDW',A.IS_ELIMINATION   )    
 ,DECODE(LENGTH(A.LEGAL_NAME),0,'NA_GDW',A.LEGAL_NAME )    
 ,DECODE(LENGTH(A.FEDERAL_NUMBER),0,'NA_GDW',A.FEDERAL_NUMBER )    
 ,DECODE(LENGTH(A.LEGAL_ENTITY_ACCOUNT_CODE),0,'NA_GDW',A.LEGAL_ENTITY_ACCOUNT_CODE   )
 ,NVL(A.FISCAL_CALENDAR_ID,-99)         
 ,DECODE(LENGTH(A.CURRENCY),0,'NA_GDW', A.CURRENCY  )
 ,NVL(A.BASE_CURRENCY_ID,-99)       
 ,DECODE(LENGTH(B.NAME),0,'NA_GDW', B.NAME)          
 ,NVL(A.PARENT_ID,-99)       
 ,sysdate
 ,'9999-12-31 23:59:59'
 ,'A'
from 
dw_prestage.subsidiaries A
,dw_prestage.subsidiaries B
WHERE A.PARENT_ID = B.subsidiary_id(+)
AND exists (select 1 from dw_prestage.subsidiaries_update
	  WHERE a.subsidiary_id = dw_prestage.subsidiaries_update.subsidiary_id
	  and dw_prestage.subsidiaries_update.ch_type = 2) ;
	  
/* dimension -> update records as part of SCD1 maintenance */

UPDATE dw.subsidiaries 
   SET  NAME                  =  DECODE(LENGTH(dw_prestage.subsidiaries.NAME),0,'NA_GDW',dw_prestage.subsidiaries.NAME)
	  , ISINACTIVE            =  DECODE(LENGTH(dw_prestage.subsidiaries.ISINACTIVE),0,'NA_GDW',dw_prestage.subsidiaries.ISINACTIVE)
	  , EDITION             =  DECODE(LENGTH(dw_prestage.subsidiaries.EDITION),0,'NA_GDW',dw_prestage.subsidiaries.EDITION)
	  , ELIMINATION      =  DECODE(LENGTH(dw_prestage.subsidiaries.IS_ELIMINATION),0,'NA_GDW',dw_prestage.subsidiaries.IS_ELIMINATION)
	  ,LEGAL_NAME        =  DECODE(LENGTH(dw_prestage.subsidiaries.LEGAL_NAME),0,'NA_GDW',dw_prestage.subsidiaries.LEGAL_NAME)
	  ,VAT_REG_NO            =  DECODE(LENGTH(dw_prestage.subsidiaries.FEDERAL_NUMBER),0,'NA_GDW',dw_prestage.subsidiaries.FEDERAL_NUMBER)
	  ,LEGAL_ACCOUNT_CODE                  =  DECODE(LENGTH(dw_prestage.subsidiaries.LEGAL_ENTITY_ACCOUNT_CODE),0,'NA_GDW',dw_prestage.subsidiaries.LEGAL_ENTITY_ACCOUNT_CODE)
   FROM dw_prestage.subsidiaries
WHERE dw.subsidiaries.subsidiary_id = dw_prestage.subsidiaries.subsidiary_id
and exists (select 1 from dw_prestage.subsidiaries_update
	  WHERE dw_prestage.subsidiaries.subsidiary_id = dw_prestage.subsidiaries_update.subsidiary_id
	  and dw_prestage.subsidiaries_update.ch_type = 1);

/* dimension -> logically delete dw records */

update dw.subsidiaries
set DATE_ACTIVE_TO = sysdate-1,
dw_active = 'I'
FROM dw_prestage.subsidiaries_delete
WHERE dw.subsidiaries.subsidiary_id = dw_prestage.subsidiaries_delete.subsidiary_id;

commit;
