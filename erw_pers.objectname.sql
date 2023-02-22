/*
 Parameters:
 Name               |   Type        |   Description
 p_debug            |   boolean     |   Default (false) parameter for debugging function no functional paramater
 p_start_read_date  |   DATE        |   Default NULL. if passed then function will process data between provided read date range.
 p_end_read_date    |   DATE        |   Default NULL. if passed then function will process data between provided read date range.


 Return: BOOLEAN
 Purpose: This function used to update (in insert and delete mode) the MDMS reads timestamps which were loaded incorrectly after ERW go-live due to IDR limitation of adding timezone offet.

       - Since ERW go-live, the reads in ERW are added by additional 7 or 8 hours.
       - So, we need to perform a data fix for historical data which are inserted after ERW-golive via regular IDR feed.
       - Exclude the data which are inserted from Hadoop as part of Initial load. By  etl_process_key we can differentiate historical load and regular IDR process load.

 Dependent Objects:
     Type    |  Name
     Table   |  erw_pers.tb_interval_read
     



 ChangeLog:
     Date       |     Author        |    Ticket         | Modification
     2022-02-21 |     Thiru K       |    ERW-39        | Initial development
 */
CREATE OR REPLACE FUNCTION erw_pers.fn_update_mdms_interval_read(p_debug boolean DEFAULT false,  p_start_read_date date DEFAULT NULL,  p_end_read_date date DEFAULT NULL)
RETURNS BOOLEAN AS
$BODY$

DECLARE
v_sql text ; 
v_msg text;
v_row_cnt bigint := 0;
v_sstart_ts timestamp with time zone;
v_fstart_ts timestamp with time zone := date_trunc('second', clock_timestamp());

--Get the ETL process key for MDMS interval reads
v_etl_process_key integer := (SELECT erw_util.fn_get_process_key('erw_pers','fn_mdms_interval_read',p_debug));

--Get the minimum ETL process date for MDMS interval reads since ERW go-live
v_min_etl_insert_ts timestamp := (SELECT COALESCE(min(parameter_value),'1900-01-01 00:00:00') FROM  erw_util.tb_etl_parameter WHERE parameter_name='mdms_interval_read_process_ts');

--Get the maximum ETL process key for ERW-39 fix deployed (this is applicable in Reads_stg) - not used for PROD and SIT
v_fix_deployment_date timestamp := (SELECT COALESCE((SELECT  date_trunc('second', deploy_date) FROM public."version"   WHERE file='0056_up.sql' AND "version"='1.0.103'), now())) ;
v_min_available_read_date Date;
v_read_start_date Date ;
v_read_end_date Date;

BEGIN

    v_sstart_ts := date_trunc('second', clock_timestamp());
    v_msg := FORMAT('%s.....Starting erw_pers.fn_update_mdms_interval_read Execution....', v_fstart_ts);
    PERFORM erw_util.logger('INFO', v_msg, p_debug);

    v_msg := FORMAT('%s.....ETL Process key for MDMS interval reads %s', v_fstart_ts, v_etl_process_key);
    PERFORM erw_util.logger('INFO', v_msg, p_debug);

    v_msg := FORMAT('%s.....Minimum available ETL timestamp for MDMS interval reads %s', v_fstart_ts, v_min_etl_insert_ts);
    PERFORM erw_util.logger('INFO', v_msg, p_debug);

    v_msg := FORMAT('%s.....ERW 39 Fix Function change deployment date for MDMS interval reads %s', v_fstart_ts, v_fix_deployment_date);
    PERFORM erw_util.logger('INFO', v_msg, p_debug);
	
	IF p_start_read_date IS NOT NULL AND p_end_read_date IS NOT NULL THEN
		v_read_start_date := p_start_read_date ;
		v_read_end_date := p_end_read_date ;
	ELSE
		v_read_start_date := '2011-01-01' ;
		v_read_end_date := current_date ;
	END IF ;
	/*
	Find the minimum read date of available reads in the erw_pers.tb_interval_read table
	*/
	v_sstart_ts := date_trunc('second', clock_timestamp());
	v_sql :=  $_$ 
				SELECT MIN(read_end_ts)
				 FROM erw_pers.tb_interval_read  
				 WHERE etl_insert_ts >= '$_$ || v_min_etl_insert_ts  || $_$' 
				 AND read_end_ts::Date BETWEEN '$_$ || v_read_start_date  || $_$' AND '$_$ || v_read_end_date  || $_$'
				 AND etl_process_key = $_$ ||  v_etl_process_key ||  $_$
			  $_$;
	PERFORM erw_util.logger('INFO', v_sql, p_debug);
	BEGIN
       IF v_sql IS NOT NULL THEN
                EXECUTE v_sql INTO v_min_available_read_date;
            ELSE
                v_msg := FORMAT('| Selecting minimum reads date from interval read table query is NULL');
                PERFORM erw_util.logger('ERROR', v_msg, p_debug);
                RETURN(FALSE);
		END IF;
		v_msg := FORMAT('| OPERATION_STATS | %s | %s | %s | %s | %s | %s | %s |', 'fn_update_mdms_interval_read', v_sstart_ts, date_trunc('second',clock_timestamp()), date_trunc('second',age(clock_timestamp(), v_sstart_ts)), v_min_available_read_date, 'SELECT', 'Minimum available reads timestamp');
        PERFORM erw_util.logger('INFO', v_msg, p_debug);
	   
	    EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION '%; SQLSTATE: %', SQLERRM, SQLSTATE;
                RETURN False;
      
    END;
	
	--
	
	RETURN True;
END

$BODY$
LANGUAGE plpgsql;
