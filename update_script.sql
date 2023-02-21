do $$


declare

v_sql text ; 
v_etl_process_key bigint ;
v_min_etl_insert_ts timestamp;  
--v_max_etl_insert_ts timestamp := select COALESCE((select  deploy_date from public."version"   where file='0056_up.sql' and "version"='1.0.103'), '9999-12-31 00:00:00') ;
v_max_etl_insert_ts timestamp := '9999-12-31' ;
v_start_ts timestamp ;
v_end_ts timestamp  :=  now();

begin
	
		
	SELECT  etl_process_key into  v_etl_process_key FROM   erw_util.tb_etl_process      WHERE    etl_process_name='fn_mdms_interval_read';
	SELECT COALESCE(min(parameter_value),'1900-01-01 00:00:00') into 	v_min_etl_insert_ts FROM  erw_util.tb_etl_parameter WHERE parameter_name='mdms_interval_read_process_ts';
	
	
	v_sql :=  $_sql_$ 
				select min(read_end_ts)
				 from erw_pers.tb_interval_read tir 
				 where etl_insert_ts >=  '$_sql_$ || v_min_etl_insert_ts  || $_sql_$'
				 and read_end_ts between '2017-01-01 00:00:00'
				 and '2021-10-01 00:00:00'
				 $_sql_$;
				
	raise notice '%', v_sql;
	
	execute v_sql into v_start_ts;
	

	
	v_sql := $_sql_$
		select 
		spid
		--   ,read_end_ts --original
		, CASE 
		WHEN EXTRACT(TIMEZONE  FROM read_end_ts ) = '-28800' THEN
		((read_end_ts- INTERVAL '28800' second))  :: timestamp with time zone 
		WHEN EXTRACT(TIMEZONE  FROM read_end_ts ) = '-25200' THEN
		((read_end_ts - INTERVAL '25200' second )):: timestamp with time zone 
		end 
		as updated_read_end_ts
		, source_commit_ts
		, etl_insert_ts
		, read_val
		, read_version_num
		, interval_length
		, read_status_id
		--, etl_process_key 
		, 999
		, channel_type_cd
		, op_code
		, uom
			, channel_type_desc
			from erw_pers.tb_interval_read
			where
			--to avoid Hadoop initial load data
			etl_process_key = $_sql_$ ||  v_etl_process_key ||  $_sql_$
			AND 
			--to consider all the data which are loaded after ERW go-live and before ERW-39 function deployment
			etl_insert_ts 
			between '$_sql_$ || v_min_etl_insert_ts || $_sql_$'
			and '$_sql_$ ||  v_max_etl_insert_ts || $_sql_$' 
			and read_end_ts between '$_sql_$ || v_start_ts || $_sql_$'
			and '$_sql_$ || v_end_ts || $_sql_$'
    $_sql_$ ;
   
  	raise notice  '%', v_sql;
   
end $$;




 