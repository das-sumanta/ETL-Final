/* prestage - drop intermediate insert table */
drop table if exists dw_prestage.payment_terms_insert;

/* prestage - create intermediate insert table*/
create table dw_prestage.payment_terms_insert
as
select * from dw_prestage.payment_terms
where exists ( select 1 from  
(select payment_terms_id from 
(select payment_terms_id from dw_prestage.payment_terms
minus
select payment_terms_id from dw_stage.payment_terms )) a
where  dw_prestage.payment_terms.payment_terms_id = a.payment_terms_id );

/* prestage - drop intermediate update table*/
drop table if exists dw_prestage.payment_terms_update;

/* prestage - create intermediate update table*/
create table dw_prestage.payment_terms_update
as
select decode(sum(ch_type),3,2,sum(ch_type)) ch_type ,payment_terms_id
from
(
SELECT payment_terms_id , CH_TYPE FROM 
(  
select payment_terms_id,NAME,PAYMENT_TERMS_EXTID,PERCENTAGE_DISCOUNT,MINIMUM_DAYS,IS_PREFERRED,ISINACTIVE,DISCOUNT_DAYS,	DAYS_UNTIL_DUE,	DATE_DRIVEN, '1' CH_TYPE from dw_prestage.payment_terms
MINUS
select payment_terms_id,NAME,PAYMENT_TERMS_EXTID,PERCENTAGE_DISCOUNT,MINIMUM_DAYS,IS_PREFERRED,ISINACTIVE,DISCOUNT_DAYS,	DAYS_UNTIL_DUE,	DATE_DRIVEN, '1' CH_TYPE from dw_stage.payment_terms
)
) a where not exists ( select 1 from dw_prestage.payment_terms_insert
where dw_prestage.payment_terms_insert.payment_terms_id = a.payment_terms_id) group by payment_terms_id;

/* prestage - drop intermediate delete table*/
drop table if exists dw_prestage.payment_terms_delete;

/* prestage - create intermediate delete table*/
create table dw_prestage.payment_terms_delete
as
select * from dw_stage.payment_terms
where exists ( select 1 from  
(select payment_terms_id from 
(select payment_terms_id from dw_stage.payment_terms
minus
select payment_terms_id from dw_prestage.payment_terms )) a
where dw_stage.payment_terms.payment_terms_id = a.payment_terms_id );

/* prestage-> stage*/
select 'no of prestage vendor records identified to inserted -->'||count(1) from  dw_prestage.payment_terms_insert;

/* prestage-> stage*/
select 'no of prestage vendor records identified to updated -->'||count(1) from  dw_prestage.payment_terms_update;

/* prestage-> stage*/
select 'no of prestage vendor records identified to deleted -->'||count(1) from  dw_prestage.payment_terms_delete;

/* stage ->delete from stage records to be updated */
delete from dw_stage.payment_terms 
using dw_prestage.payment_terms_update
where dw_stage.payment_terms.payment_terms_id = dw_prestage.payment_terms_update.payment_terms_id;

/* stage ->delete from stage records which have been deleted */
delete from dw_stage.payment_terms 
using dw_prestage.payment_terms_delete
where dw_stage.payment_terms.payment_terms_id = dw_prestage.payment_terms_delete.payment_terms_id;

/* stage ->insert into stage records which have been created */
insert into dw_stage.payment_terms(DATE_DRIVEN , DATE_LAST_MODIFIED , DAYS_UNTIL_DUE , DISCOUNT_DAYS , ISINACTIVE , IS_PREFERRED , MINIMUM_DAYS , NAME , PAYMENT_TERMS_EXTID , PAYMENT_TERMS_ID , PERCENTAGE_DISCOUNT) select DATE_DRIVEN , DATE_LAST_MODIFIED , DAYS_UNTIL_DUE , DISCOUNT_DAYS , ISINACTIVE , IS_PREFERRED , MINIMUM_DAYS , NAME , PAYMENT_TERMS_EXTID , PAYMENT_TERMS_ID , PERCENTAGE_DISCOUNT from dw_prestage.payment_terms_insert;

/* stage ->insert into stage records which have been updated */
insert into dw_stage.payment_terms (DATE_DRIVEN , DATE_LAST_MODIFIED , DAYS_UNTIL_DUE , DISCOUNT_DAYS , ISINACTIVE , IS_PREFERRED , MINIMUM_DAYS , NAME , PAYMENT_TERMS_EXTID , PAYMENT_TERMS_ID , PERCENTAGE_DISCOUNT)
select DATE_DRIVEN , DATE_LAST_MODIFIED , DAYS_UNTIL_DUE , DISCOUNT_DAYS , ISINACTIVE , IS_PREFERRED , MINIMUM_DAYS , NAME , PAYMENT_TERMS_EXTID , PAYMENT_TERMS_ID , PERCENTAGE_DISCOUNT from dw_prestage.payment_terms
where exists ( select 1 from 
dw_prestage.payment_terms_update
where dw_prestage.payment_terms_update.payment_terms_id = dw_prestage.payment_terms.payment_terms_id);

commit;

/* dimension ->insert new records in dim payment_terms */

insert into dw.payment_terms ( 
  payment_terms_id,
  NAME,
  PAYMENT_TERMS_EXTID,
  PERCENTAGE_DISCOUNT,
  MINIMUM_DAYS,
  IS_PREFERRED,
  ISINACTIVE,
  DISCOUNT_DAYS,
  DAYS_UNTIL_DUE,
  DATE_DRIVEN,
  DATE_ACTIVE_FROM,
  DATE_ACTIVE_TO,
  DW_ACTIVE
  )
select 
 payment_terms_id
 ,DECODE(LENGTH(NAME),0,'NA_GDW',NAME)
 ,DECODE(LENGTH(PAYMENT_TERMS_EXTID),0,'NA_GDW',PAYMENT_TERMS_EXTID )         
 ,NVL(PERCENTAGE_DISCOUNT,-99)      
 ,NVL(MINIMUM_DAYS,-99)    
 ,DECODE(LENGTH(IS_PREFERRED),0,'NA_GDW', IS_PREFERRED )    
 ,DECODE(LENGTH(ISINACTIVE),0,'NA_GDW', ISINACTIVE )    
 ,NVL(DISCOUNT_DAYS,-99)       
 ,NVL(DAYS_UNTIL_DUE,-99)
 ,DECODE(LENGTH(DATE_DRIVEN),0,'NA_GDW', DATE_DRIVEN )   
 ,sysdate
 ,'9999-12-31 23:59:59'
 ,'A'
  from 
dw_prestage.payment_terms_insert A;


/* dimension ->update old record as part of SCD2 maintenance*/

-- UPDATE dw.payment_terms
--   SET dw_active = 'I' ,
--	   DATE_ACTIVE_TO = (sysdate -1)
-- WHERE dw_active = 'A'
--      and sysdate >= date_active_from and sysdate < date_active_to
--	  and exists ( select 1 from dw_prestage.payment_terms_update
--	  WHERE dw.payment_terms.payment_terms_id = dw_prestage.payment_terms_update.payment_terms_id
--	  and dw_prestage.payment_terms_update.ch_type = 2);  

/* dimension -> Insert the new records as part of SCD2 maintenance*/

--insert into dw.payment_terms ( 
--   payment_terms_id        
--  ,NAME
--  ,FULL_NAME
--  ,DESCRIPTION
--  ,INCOME_ACCOUNT_ID
--  ,INCOME_ACCOUNT_NUMBER
--  ,INCOME_ACCOUNT_NAME
--  ,ISINACTIVE
--  ,RATE
--  ,DATE_ACTIVE_FROM
--  ,DATE_ACTIVE_TO
--  ,DW_ACTIVE )
-- select 
--  payment_terms_id
-- ,DECODE(LENGTH(NAME),0,'NA_GDW',NAME)
-- ,DECODE(LENGTH(FULL_NAME),0,'NA_GDW',FULL_NAME )         
--,DECODE(LENGTH(DESCRIPTION),0,'NA_GDW',DESCRIPTION   )      
--,NVL(INCOME_ACCOUNT_ID,-99)    
--,DECODE(LENGTH(INCOME_ACCOUNT_NUMBER),0,'NA_GDW', INCOME_ACCOUNT_NUMBER )    
--,DECODE(LENGTH(INCOME_ACCOUNT_NAME),0,'NA_GDW', INCOME_ACCOUNT_NAME )    
--,NVL(ISINACTIVE,'NA_GDW')       
--,DECODE(LENGTH(RATE),0,'NA_GDW', RATE) 
--,sysdate
--,'9999-12-31 23:59:59'
--,'A'
-- from 
-- dw_prestage.payment_terms A
-- WHERE exists (select 1 from dw_prestage.payment_terms_update
--	  WHERE a.payment_terms_id = dw_prestage.payment_terms_update.payment_terms_id
--	  and dw_prestage.payment_terms_update.ch_type = 2) ;  
	  
/* dimension ->update SCD1 */

UPDATE dw.payment_terms 
   SET  NAME                  =  DECODE(LENGTH(dw_prestage.payment_terms.NAME),0,'NA_GDW',dw_prestage.payment_terms.NAME)
	  , PAYMENT_TERMS_EXTID                 =  DECODE(LENGTH(dw_prestage.payment_terms.PAYMENT_TERMS_EXTID),0,'NA_GDW',dw_prestage.payment_terms.PAYMENT_TERMS_EXTID)
	  , PERCENTAGE_DISCOUNT             =  NVL(dw_prestage.payment_terms.PERCENTAGE_DISCOUNT,-99)
	  , MINIMUM_DAYS             =  NVL(dw_prestage.payment_terms.MINIMUM_DAYS,-99)
	  , DISCOUNT_DAYS             =  NVL(dw_prestage.payment_terms.DISCOUNT_DAYS,-99)
	  , DAYS_UNTIL_DUE             =  NVL(dw_prestage.payment_terms.DAYS_UNTIL_DUE,-99)
	  ,IS_PREFERRED            =  DECODE(LENGTH(dw_prestage.payment_terms.IS_PREFERRED),0,'NA_GDW',dw_prestage.payment_terms.IS_PREFERRED)
	  ,ISINACTIVE            =  DECODE(LENGTH(dw_prestage.payment_terms.ISINACTIVE),0,'NA_GDW',dw_prestage.payment_terms.ISINACTIVE)
	  ,DATE_DRIVEN            =  DECODE(LENGTH(dw_prestage.payment_terms.DATE_DRIVEN),0,'NA_GDW',dw_prestage.payment_terms.DATE_DRIVEN)
   FROM dw_prestage.payment_terms
WHERE dw.payment_terms.payment_terms_id = dw_prestage.payment_terms.payment_terms_id
and exists (select 1 from dw_prestage.payment_terms_update
	  WHERE dw_prestage.payment_terms.payment_terms_id = dw_prestage.payment_terms_update.payment_terms_id
	  and dw_prestage.payment_terms_update.ch_type = 1);

/* dimension ->logically delete dw records */
update dw.payment_terms
set DATE_ACTIVE_TO = sysdate-1,
dw_active = 'I'
FROM dw_prestage.payment_terms_delete
WHERE dw.payment_terms.payment_terms_id = dw_prestage.payment_terms_delete.payment_terms_id;

commit;
