/* prestage - drop intermediate insert table */
drop table if exists dw_prestage.currencies_insert;

/* prestage - create intermediate insert table*/
create table dw_prestage.currencies_insert
as
select * from dw_prestage.currencies
where exists ( select 1 from  
(select currency_id from 
(select currency_id from dw_prestage.currencies
minus
select currency_id from dw_stage.currencies )) a
where  dw_prestage.currencies.currency_id = a.currency_id );

/* prestage - drop intermediate update table*/
drop table if exists dw_prestage.currencies_update;

/* prestage - create intermediate update table*/
create table dw_prestage.currencies_update
as
select decode(sum(ch_type),3,2,sum(ch_type)) ch_type ,currency_id
from
(
SELECT currency_id , CH_TYPE FROM 
(  
select currency_id,NAME,CURRENCY_EXTID,IS_INACTIVE,PRECISION_0,SYMBOL, '1' CH_TYPE from dw_prestage.currencies
MINUS
select currency_id,NAME,CURRENCY_EXTID,IS_INACTIVE,PRECISION_0,SYMBOL, '1' CH_TYPE from dw_stage.currencies
)
) a where not exists ( select 1 from dw_prestage.currencies_insert
where dw_prestage.currencies_insert.currency_id = a.currency_id) group by currency_id;

/* prestage - drop intermediate delete table*/
drop table if exists dw_prestage.currencies_delete;

/* prestage - create intermediate delete table*/
create table dw_prestage.currencies_delete
as
select * from dw_stage.currencies
where exists ( select 1 from  
(select currency_id from 
(select currency_id from dw_stage.currencies
minus
select currency_id from dw_prestage.currencies )) a
where dw_stage.currencies.currency_id = a.currency_id );

/* prestage-> stage*/
select 'no of prestage currency records identified to inserted -->'||count(1) from  dw_prestage.currencies_insert;

/* prestage-> stage*/
select 'no of prestage currency records identified to updated -->'||count(1) from  dw_prestage.currencies_update;

/* prestage-> stage*/
select 'no of prestage currency records identified to deleted -->'||count(1) from  dw_prestage.currencies_delete;

/*  stage ->delete from stage records to be updated */
delete from dw_stage.currencies 
using dw_prestage.currencies_update
where dw_stage.currencies.currency_id = dw_prestage.currencies_update.currency_id;

/*  stage ->delete from stage records which have been deleted */
delete from dw_stage.currencies 
using dw_prestage.currencies_delete
where dw_stage.currencies.currency_id = dw_prestage.currencies_delete.currency_id;

/*  stage ->insert into stage records which have been created */
insert into dw_stage.currencies(CURRENCY_EXTID,CURRENCY_ID,DATE_LAST_MODIFIED,IS_INACTIVE,NAME,PRECISION_0,SYMBOL) 
select CURRENCY_EXTID,CURRENCY_ID,DATE_LAST_MODIFIED,IS_INACTIVE,NAME,PRECISION_0,SYMBOL from dw_prestage.currencies_insert;

/*  stage ->insert into stage records which have been updated */
insert into dw_stage.currencies (CURRENCY_EXTID,CURRENCY_ID,DATE_LAST_MODIFIED,IS_INACTIVE,NAME,PRECISION_0,SYMBOL)
select CURRENCY_EXTID,CURRENCY_ID,DATE_LAST_MODIFIED,IS_INACTIVE,NAME,PRECISION_0,SYMBOL from dw_prestage.currencies
where exists ( select 1 from 
dw_prestage.currencies_update
where dw_prestage.currencies_update.currency_id = dw_prestage.currencies.currency_id);

commit;

/* dimension ->insert new records in dim currencies */

insert into dw.currencies (      
 CURRENCY_ID       
,CURRENCY_EXTID	   
,ISINACTIVE        
,NAME              
,PRECISION      
,DATE_ACTIVE_FROM  
,DATE_ACTIVE_TO    
,DW_ACTIVE         
,SYMBOL )
select 
A.CURRENCY_ID,
DECODE(LENGTH(A.CURRENCY_EXTID),0,'NA_GDW',A.CURRENCY_EXTID),
A.IS_INACTIVE,
NVL(A.NAME,'NA_GDW'),
NVL(A.PRECISION_0,-99),
sysdate,
'9999-12-31 23:59:59',
'A',
NVL(A.SYMBOL,'NA_GDW')
from 
dw_prestage.currencies_insert A;

UPDATE dw.currencies
   SET name = dw_prestage.currencies.name,
       isinactive = dw_prestage.currencies.is_inactive,    
	   CURRENCY_EXTID = DECODE(LENGTH(dw_prestage.currencies.CURRENCY_EXTID),0,'NA_GDW',dw_prestage.currencies.CURRENCY_EXTID),
	   PRECISION = dw_prestage.currencies.PRECISION_0,
	   SYMBOL = NVL(dw_prestage.currencies.SYMBOL,'NA_GDW')
FROM dw_prestage.currencies
WHERE dw.currencies.currency_id = dw_prestage.currencies.currency_id
and exists (select 1 from dw_prestage.currencies_update
	  WHERE dw_prestage.currencies.currency_id = dw_prestage.currencies_update.currency_id
	  and dw_prestage.currencies_update.ch_type = 1);

/* dimension ->logically delete dw records */
update dw.currencies
set DATE_ACTIVE_TO = sysdate-1,
dw_active = 'I'
FROM dw_prestage.currencies_delete
WHERE dw.currencies.currency_id = dw_prestage.currencies_delete.currency_id;

commit;