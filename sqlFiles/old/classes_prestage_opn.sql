/* prestage - drop intermediate insert table */
drop table if exists dw_prestage.classes_insert;

/* prestage - create intermediate insert table*/
create table dw_prestage.classes_insert
as
select * from dw_prestage.classes
where exists ( select 1 from  
(select class_id from 
(select class_id from dw_prestage.classes
minus
select class_id from dw_stage.classes )) a
where  dw_prestage.classes.class_id = a.class_id );

/* prestage - drop intermediate update table*/
drop table if exists dw_prestage.classes_update;

/* prestage - create intermediate update table*/
create table dw_prestage.classes_update
as
select decode(sum(ch_type),3,2,sum(ch_type)) ch_type ,class_id
from
(
SELECT CLASS_ID , CH_TYPE FROM 
(  
select CLASS_ID , PARENT_ID , ACCPAC_LOB_CODES , HYPERION_LOB_CODES ,LINE_OF_BUSINESS_CODE, '2' CH_TYPE from dw_prestage.classes
MINUS
select CLASS_ID , PARENT_ID , ACCPAC_LOB_CODES , HYPERION_LOB_CODES ,LINE_OF_BUSINESS_CODE, '2' CH_TYPE from dw_stage.classes
)
union all
SELECT CLASS_ID , CH_TYPE FROM 
(  
select CLASS_ID,name , isinactive , '1' CH_TYPE from dw_prestage.classes
MINUS
select CLASS_ID,name , isinactive , '1' CH_TYPE from dw_stage.classes
)
) a where not exists ( select 1 from dw_prestage.classes_insert
where dw_prestage.classes_insert.class_id = a.class_id) group by class_id;

/* prestage - drop intermediate delete table*/
drop table if exists dw_prestage.classes_delete;

/* prestage - create intermediate delete table*/
create table dw_prestage.classes_delete
as
select * from dw_stage.classes
where exists ( select 1 from  
(select class_id from 
(select class_id from dw_stage.classes
minus
select class_id from dw_prestage.classes )) a
where dw_stage.classes.class_id = a.class_id );

/* prestage-> stage*/
select 'no of prestage class records identified to inserted -->'||count(1) from  dw_prestage.classes_insert;

/* prestage-> stage*/
select 'no of prestage class records identified to updated -->'||count(1) from  dw_prestage.classes_update;

/* prestage-> stage*/
select 'no of prestage class records identified to deleted -->'||count(1) from  dw_prestage.classes_delete;

/* stage ->delete from stage records to be updated */
delete from dw_stage.classes 
using dw_prestage.classes_update
where dw_stage.classes.class_id = dw_prestage.classes_update.class_id;

/* stage ->delete from stage records which have been deleted */
delete from dw_stage.classes 
using dw_prestage.classes_delete
where dw_stage.classes.class_id = dw_prestage.classes_delete.class_id;

/* stage ->insert into stage records which have been created */
insert into dw_stage.classes (ACCPAC_LOB_CODES,CLASS_EXTID,CLASS_ID,DATE_LAST_MODIFIED,FULL_NAME,HYPERION_LOB_CODES,ISINACTIVE,LINE_OF_BUSINESS_CODE,NAME,PARENT_ID)
select ACCPAC_LOB_CODES,CLASS_EXTID,CLASS_ID,DATE_LAST_MODIFIED,FULL_NAME,HYPERION_LOB_CODES,ISINACTIVE,LINE_OF_BUSINESS_CODE,NAME,PARENT_ID from dw_prestage.classes_insert;

/* stage ->insert into stage records which have been updated */
insert into dw_stage.classes (ACCPAC_LOB_CODES,CLASS_EXTID,CLASS_ID,DATE_LAST_MODIFIED,FULL_NAME,HYPERION_LOB_CODES,ISINACTIVE,LINE_OF_BUSINESS_CODE,NAME,PARENT_ID)
select ACCPAC_LOB_CODES,CLASS_EXTID,CLASS_ID,DATE_LAST_MODIFIED,FULL_NAME,HYPERION_LOB_CODES,ISINACTIVE,LINE_OF_BUSINESS_CODE,NAME,PARENT_ID from dw_prestage.classes
where exists ( select 1 from 
dw_prestage.classes_update
where dw_prestage.classes_update.class_id = dw_prestage.classes.class_id);

commit;


/* dimension ->insert new records in dim classes */

insert into dw.classes ( 
 CLASS_ID   
,ISINACTIVE             
,LINE_OF_BUSINESS_CODE 
,ACCPAC_LOB_CODES
,HYPERION_LOB_CODES 
,NAME                   
,PARENT_ID              
,date_active_from       
,DATE_ACTIVE_TO         
,dw_active              
,PARENT_NAME     )
select 
A.CLASS_ID,
A.ISINACTIVE,
DECODE(LENGTH(A.LINE_OF_BUSINESS_CODE),0,'NA_GDW',A.LINE_OF_BUSINESS_CODE),
DECODE(LENGTH(A.ACCPAC_LOB_CODES),0,'NA_GDW',A.ACCPAC_LOB_CODES),   
DECODE(LENGTH(A.HYPERION_LOB_CODES),0,'NA_GDW',A.HYPERION_LOB_CODES),   
A.NAME,
NVL(A.PARENT_ID,-99),
sysdate,
'9999-12-31 23:59:59',
'A',
NVL(b.name,'NA_GDW')
from 
dw_prestage.classes_insert A,
dw_prestage.classes B
WHERE a.parent_id = b.class_id(+);

/* dimension ->update old record as part of SCD2 maintenance*/

UPDATE dw.classes
   SET dw_active = 'I' ,
	   DATE_ACTIVE_TO = (sysdate -1)
WHERE dw_active = 'A'
      and sysdate >= date_active_from and sysdate < date_active_to
	  and exists ( select 1 from dw_prestage.classes_update
	  WHERE dw.classes.class_id = dw_prestage.classes_update.class_id
	  and dw_prestage.classes_update.ch_type = 2);

/* dimension ->insert the new records as part of SCD2 maintenance*/

insert into dw.classes ( 
 CLASS_ID   
,ISINACTIVE             
,LINE_OF_BUSINESS_CODE  
,ACCPAC_LOB_CODES
,HYPERION_LOB_CODES 
,NAME                   
,PARENT_ID              
,date_active_from       
,DATE_ACTIVE_TO         
,dw_active              
,PARENT_NAME     )
select 
A.CLASS_ID,
A.ISINACTIVE,
DECODE(LENGTH(A.LINE_OF_BUSINESS_CODE),0,'NA_GDW',A.LINE_OF_BUSINESS_CODE),
DECODE(LENGTH(A.ACCPAC_LOB_CODES),0,'NA_GDW',A.ACCPAC_LOB_CODES),   
DECODE(LENGTH(A.HYPERION_LOB_CODES),0,'NA_GDW',A.HYPERION_LOB_CODES),  
A.NAME,
NVL(A.PARENT_ID,-99),
sysdate,
'9999-12-31 23:59:59',
'A',
NVL(b.name,'NA_GDW')
from 
dw_prestage.classes A,
dw_prestage.classes B
WHERE a.parent_id = b.class_id(+)
and exists (select 1 from dw_prestage.classes_update
	  WHERE a.class_id = dw_prestage.classes_update.class_id
	  and dw_prestage.classes_update.ch_type = 2) ;
	  
/* dimension -> update records as part of SCD1 maintenance */

UPDATE dw.classes
   SET name = dw_prestage.classes.name,
       isinactive = dw_prestage.classes.isinactive
FROM dw_prestage.classes
WHERE dw.classes.class_id = dw_prestage.classes.class_id
and exists (select 1 from dw_prestage.classes_update
	  WHERE dw_prestage.classes.class_id = dw_prestage.classes_update.class_id
	  and dw_prestage.classes_update.ch_type = 1);

/* dimension ->logically delete dw records */
update dw.classes
set DATE_ACTIVE_TO = sysdate-1,
dw_active = 'I'
FROM dw_prestage.classes_delete
WHERE dw.classes.class_id = dw_prestage.classes_delete.class_id;

commit;
