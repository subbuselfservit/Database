
create PROCEDURE [dbo].[sp_retrieve_my_calls] 
    @i_session_id [sessionid], 
    @i_user_id [userid], 
    @i_client_id [uddt_client_id], 
    @i_locale_id [uddt_locale_id], 
    @i_country_code [uddt_country_code], 
    @i_inputparam_xml [uddt_nvarchar_max],
    @o_retrieve_status [uddt_varchar_5] OUTPUT
AS
BEGIN
    /*
     * retrieve call list list for a user
     */
    -- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
    SET NOCOUNT ON;

    -- The following SQL snippet illustrates (with sample values) assignment of scalar output parameters
    -- returned out of this stored procedure

    -- Use SET | SELECT for assigning values
    /*
    SET 
         @o_retrieve_status = '' /* string */
     */

    /*
    -- The following SQL snippet illustrates selection of result sets expected from this stored procedure: 
    
    -- Result set 1: call_list

    SELECT
        '' as call_list, /* dummy column aliased by result set name */
        '' as o_call_xml /* unicode string */
    FROM <Table name>
    */

	
	declare @p_inputparam_xml xml, @p_equipment_id nvarchar(30), @p_customer_id varchar(15),
			@p_checksum_ind varchar(15),@p_user_group_type varchar(2),@p_assigned_to_empid nvarchar(12)
	
	declare @o_udf_char_1 bit,	@o_udf_char_2 bit,	@o_udf_char_3 bit,	@o_udf_char_4  bit,
			@o_udf_char_5 bit,	@o_udf_char_6 bit,	@o_udf_char_7 bit,	@o_udf_char_8  bit,
			@o_udf_char_9 bit,	@o_udf_char_10 bit,
			@o_udf_bit_1 bit,	@o_udf_bit_2 bit,	@o_udf_bit_3 bit,	@o_udf_bit_4 bit,
			@o_udf_float_1 bit,	@o_udf_float_2 bit,	@o_udf_float_3 bit,	@o_udf_float_4 bit,
			@o_udf_date_1 bit,	@o_udf_date_2 bit,	@o_udf_date_3  bit,	@o_udf_date_4  bit,
			@o_udf_analysis_code1 bit,	@o_udf_analysis_code2 bit,	@o_udf_analysis_code3 bit,
			@o_udf_analysis_code4 bit
	
	
	set @p_inputparam_xml = CAST(@i_inputparam_xml as XML)
	
	create table #input_params (
		paramname varchar(50) not null,
		paramval varchar(50) null
	)

	create table #input_params_multival (
		paramname varchar(50) not null,
		paramval varchar(50) not null
	)
  
	insert #input_params (
		paramname, paramval
	)
	SELECT nodes.value('local-name(.)', 'varchar(50)'),
		nodes.value('(.)[1]', 'varchar(50)')
	FROM @p_inputparam_xml.nodes('/inputparam/*') AS Tbl(nodes)


	update #input_params
	set paramval = '%' 
	where paramval = 'ALL' or paramval = ''

	select @p_assigned_to_empid = paramval from #input_params where paramname = 'assigned_to_emp_id_filter'

	/*
	  Get list of udfs configured
	*/  
	execute sp_retrieve_applicable_udfs @i_client_id , @i_country_code ,  @i_session_id , 
		@i_user_id ,  @i_locale_id , 'CALLREGISTER',@o_udf_char_1 OUTPUT,
		@o_udf_char_2 OUTPUT,@o_udf_char_3 OUTPUT,	@o_udf_char_4  OUTPUT,
		@o_udf_char_5 OUTPUT,@o_udf_char_6 OUTPUT,	@o_udf_char_7  OUTPUT,
		@o_udf_char_8 OUTPUT,@o_udf_char_9 OUTPUT,	@o_udf_char_10  OUTPUT,
		@o_udf_bit_1 OUTPUT,@o_udf_bit_2 OUTPUT,@o_udf_bit_3 OUTPUT,
		@o_udf_bit_4 OUTPUT,@o_udf_float_1 OUTPUT,	@o_udf_float_2 OUTPUT,
		@o_udf_float_3 OUTPUT,	@o_udf_float_4 OUTPUT,	@o_udf_date_1 OUTPUT,
		@o_udf_date_2 OUTPUT,@o_udf_date_3 OUTPUT,	@o_udf_date_4 OUTPUT,
		@o_udf_analysis_code1 OUTPUT,	@o_udf_analysis_code2 OUTPUT,
		@o_udf_analysis_code3 OUTPUT,	@o_udf_analysis_code4 OUTPUT
	
	create table #call_list (
		call_ref_no nvarchar(20) not null,
		call_wf_stage_no tinyint not null,
		call_status varchar(2) not null,
		customer_id varchar(15) null,
		customer_location_code varchar(10) null,
		customer_name nvarchar(100) null,
		created_on_date datetimeoffset(7) null,
		assigned_on_date datetimeoffset(7) null,
		sch_start_date datetimeoffset(7) null,
		sch_finish_date datetimeoffset(7) null,
		act_start_date datetimeoffset(7) null,
		act_finish_date datetimeoffset(7) null,
		closed_on_date datetimeoffset(7) null,
		call_xml nvarchar(max) not null,
		call_assigned_to_emp_id nvarchar(12) null,
		call_assigned_to_emp_name nvarchar(100) null,
		delay_in_hrs_same_status int null
	)

	
		insert #call_list (
			call_ref_no, call_wf_stage_no, call_status, 
			customer_id, customer_location_code,
			created_on_date, sch_start_date, sch_finish_date,
			act_start_date, act_finish_date,closed_on_date,
			call_xml
		)
		select top(200) a.call_ref_no, 
			a.call_wf_stage_no, a.call_status,
			isnull(a.customer_id,''), isnull(a.customer_location_code,''),
			isnull(a.created_on_date,''), isnull(a.sch_start_date,''),
			isnull(a.sch_finish_date,''), isnull(a.act_start_date,''),
			isnull(a.act_finish_date,''), isnull(a.closed_on_date,''), 
		'{' +
			'"cust_location_code":"'+a.customer_location_code+'",'+
			'"call_no":"'+isnull(a.call_ref_no,'')+'",'+
			'"call_category":"'+a.call_category+'",'+
			'"call_category_desc":"'+
				case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLCATG'
						  and f.code = a.call_category)
					when 1 then
					(select isnull(e.short_description,'') 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLCATG'
					  and e.code = a.call_category)
					else
					(select isnull(g.short_description,'') from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLCATG'
					  and g.code = a.call_category)
				 end +
				'",'+
			'"call_type":"'+a.call_type+'",'+ 
			'"call_type_desc":"'+
				isnull(case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLTYPE'
						  and f.code = a.call_type)
					when 1 then
					(select isnull(e.short_description,'') 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLTYPE'
					  and e.code = a.call_type)
					else
					(select isnull(g.short_description,'') from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLTYPE'
					  and g.code = a.call_type)
				 end,'') +
				'",'+
			'"call_wf_stage":"'+ convert(varchar(2), a.call_wf_stage_no)+'",'+
			'"call_wf_stage_desc":"'+
		   (
			 isnull((select isnull(x.description,'') 
			 from workflow_stage_master x
			 where x.company_id = @i_client_id
			   and x.country_code = @i_country_code
			   and x.transaction_type_code = 'CALL'
			   and x.request_category = a.call_category
			   and x.workflow_stage_no = a.call_wf_stage_no),'')
			)
		   +'",'+ 
			'"call_status":"'+a.call_status+'",'+
			'"call_status_desc":"'+
				case (select 1 from code_table_mlingual_translation f
				where f.company_id = @i_client_id
					and f.country_code = @i_country_code
					and f.locale_id = @i_locale_id
					and f.code_type = 'CALLSTATUS'
					and f.code = a.call_status)
				when 1 then
				(select e.long_description 
				from code_table_mlingual_translation e
				where e.company_id = @i_client_id
					and e.country_code = @i_country_code
					and e.locale_id = @i_locale_id
					and e.code_type = 'CALLSTATUS'
					and e.code = a.call_status)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
					and g.country_code = @i_country_code
					and g.locale_id = 'ALL'
					and g.code_type = 'CALLSTATUS'
					and g.code = a.call_status)
				end + '",'+
			'"priority_cd":"'+a.priority_code+'",'+
			'"company_location_code":"'+ a.company_location_code+'",'+
			'"asset_id":"'+a.asset_id+	'",'+
			'"active_call":"'+ 
				case (select 1 
				from employee_lastaccess_info b
				where a.company_id = b.company_id
					and a.country_code = b.country_code
					and isnull(b.employee_id,'') = @p_assigned_to_empid
					and b.last_accessed_txn_ref_no = a.call_ref_no
					and b.allow_newtxn_ind = 0
				)
				when 1 then 'T'
				else 'F'
				end + '",' + 
			'"problem_description":"'+isnull(a.problem_description,'')+'",'+
			'"No_of_trips":"'+cast(isnull((select count(*) from trip_sheet ts
				where ts.company_id = @i_client_id
					and ts.country_code = @i_country_code
					and ts.trip_for_ind = 'C'
					and isnull(ts.employee_id,'') = @p_assigned_to_empid
					and ts.call_ref_no = a.call_ref_no ),0) as varchar(4))+'",'+
			'"total_trip_distance_in_kms":"'+cast(isnull((select SUM(distance_in_kms) from trip_sheet ts1
				where ts1.company_id = @i_client_id
					and ts1.country_code = @i_country_code
					and ts1.trip_for_ind = 'C'
					and isnull(ts1.employee_id,'') = @p_assigned_to_empid
					and ts1.call_ref_no = a.call_ref_no ),0) as varchar(66))+'",'+
			'"total_travel_time_in_hrs":"'+cast(isnull((select sum(DATEdiff(hh, ts2.trip_finish_datetime, ts2.trip_start_datetime))
				from trip_sheet ts2
				where ts2.company_id = @i_client_id
					and ts2.country_code = @i_country_code
					and ts2.trip_for_ind = 'C'
					and isnull(ts2.employee_id,'') = @p_assigned_to_empid
					and ts2.call_ref_no = a.call_ref_no ),0) as varchar(6))+'",'+
			'"total_effort_in_hrs":"'+cast(isnull((
				select SUM(datediff(hh, crul.to_date, crul.from_date))
				from call_resource_utilisation_log crul
				where crul.company_id = @i_client_id
				and crul.country_code = @i_country_code
				and crul.call_ref_no = a.call_ref_no
				and isnull(crul.resource_emp_id,'') = @p_assigned_to_empid
				),0) as varchar(6))+'",'+
			'"addn_desc":"'+ISNULL(a.additional_information,'')+'",'+ /* this may overflow */
			'"company_location_name":"'+
			   isnull((select cl2.location_name_short 
						from company_location cl2
						where cl2.company_id = @i_client_id
						  and cl2.country_code = @i_country_code
						  and cl2.location_code = a.company_location_code),'')
			   +'",'+
			'"cust_contact_name":"'+isnull(a.customer_contact_name,'')+'",'+
			'"cust_contact_no":"'+isnull(a.customer_contact_no,'')+'",'+
			'"cust_contact_email_id":"'+isnull(a.customer_contact_email_id,'')+'",'+
			'"customer_id":"'+ISNULL(a.customer_id,'')+'",'+
			'"asset_loc_reported":"'+isnull(a.asset_location_code_reported,'')+'",'+
			'"equipment_id":"'+ISNULL(a.equipment_id,'')+'",'+
			'"created_on_date":"'+isnull(CONVERT(varchar(10),a.created_on_date,120),'')+'",'+
			'"created_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.created_on_date,108),1,2),'')+'",'+
			'"created_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.created_on_date,108),4,2),'')+'",'+
			'"closed_on_date":"'+isnull(CONVERT(varchar(10),a.closed_on_date,120),'')+'",'+
			'"closed_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.closed_on_date,108),1,2),'')+'",'+
			'"closed_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.closed_on_date,108),4,2),'')+'",'+       
			'"sch_start_on_date":"'+isnull(CONVERT(varchar(10),a.sch_start_date,120),'')+'",'+
			'"sch_start_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.sch_start_date,108),1,2),'')+'",'+
			'"sch_start_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.sch_start_date,108),4,2),'')+'",'+       
			'"sch_finish_on_date":"'+isnull(CONVERT(varchar(10),a.sch_finish_date,120),'')+'",'+
			'"sch_finish_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.sch_finish_date,108),1,2),'')+'",'+
			'"sch_finish_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.sch_finish_date,108),4,2),'')+'",'+       
			'"plan_duration":"'+isnull(CAST(a.plan_duration as varchar(8)),'')+'",'+
			'"plan_duration_uom":"'+isnull(CAST(a.plan_duration_uom as varchar(3)),'')+'",'+
			'"plan_duration_uom_desc":"'+ISNULL(
			case (select 1 from code_table_mlingual_translation f
					where f.company_id = @i_client_id
					  and f.country_code = @i_country_code
					  and f.locale_id = @i_locale_id
					  and f.code_type = 'TDURUOM'
					  and f.code = a.plan_duration_uom)
				when 1 then
				(select e.long_description 
					from code_table_mlingual_translation e
				where e.company_id = @i_client_id
				  and e.country_code = @i_country_code
				  and e.locale_id = @i_locale_id
				  and e.code_type = 'TDURUOM'
				  and e.code = a.plan_duration_uom)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
				  and g.country_code = @i_country_code
				  and g.locale_id = 'ALL'
				  and g.code_type = 'TDURUOM'
				  and g.code = a.plan_duration_uom)
			 end,'') +
			'",'+
			'"plan_work":"'+isnull(CAST(a.plan_work as varchar(8)),'')+'",'+
			'"plan_work_uom":"'+isnull(CAST(a.plan_work_uom as varchar(3)),'')+'",'+
			'"plan_work_uom_desc":"'+ ISNULL(
			case (select 1 from code_table_mlingual_translation f
					where f.company_id = @i_client_id
					  and f.country_code = @i_country_code
					  and f.locale_id = @i_locale_id
					  and f.code_type = 'TWORKUOM'
					  and f.code = a.plan_work_uom)
				when 1 then
				(select e.long_description 
					from code_table_mlingual_translation e
				where e.company_id = @i_client_id
				  and e.country_code = @i_country_code
				  and e.locale_id = @i_locale_id
				  and e.code_type = 'TWORKUOM'
				  and e.code = a.plan_work_uom)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
				  and g.country_code = @i_country_code
				  and g.locale_id = 'ALL'
				  and g.code_type = 'TWORKUOM'
				  and g.code = a.plan_work_uom)
			 end,'') +
			'",'+
			'"act_start_on_date":"'+isnull(CONVERT(varchar(10),a.act_start_date,120),'')+'",'+
			'"act_start_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),1,2),'')+'",'+
			'"act_start_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),4,2),'')+'",'+       
			'"act_finish_on_date":"'+isnull(CONVERT(varchar(10),a.act_finish_date,120),'')+'",'+
			'"act_finish_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),1,2),'')+'",'+
			'"act_finish_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),4,2),'')+'",'+      
			'"act_duration":"'+isnull(CAST(a.actual_duration as varchar(8)),'')+'",'+
			'"act_work":"'+isnull(CAST(a.actual_work as varchar(8)),'')+'",'+
			'"head_emp_name":"'+
			ISNULL((select e1.title+'.'+e1.first_name+ISNULL(e1.middle_name,'')+e1.last_name
					from employee e1 JOIN company_location cl
					ON e1.employee_id = cl.head_emp_id
					where e1.company_id = @i_client_id
					and e1.country_code = @i_country_code
					and cl.company_id = e1.company_id
					and cl.country_code = e1.country_code
					and cl.location_code = a.company_location_code),'')
			+'",'+
			'"head_emp_contact":"'+
			isnull((select e1.contact_mobile_no
					from employee e1 JOIN company_location cl
					ON e1.employee_id = cl.head_emp_id
					where e1.company_id = @i_client_id
					and e1.country_code = @i_country_code
					and cl.company_id = e1.company_id
					and cl.country_code = e1.country_code
					and cl.location_code = a.company_location_code ),'')
			+'",'+
			'"last_accessed_feature":"' + isnull((
				select ftr.feature_id   
				from company_feature ftr
				where ftr.company_id = @i_client_id
					and ftr.country_code = @i_country_code
					and ftr.channel_id = 'Mobile'
					and ftr.screen_id = (
						select top(1) (case(csel.eventverb_id)
							when null then ''
							when 'Trip start' then 'trip_start'
							when 'Trip finish' then 'trip_finish'
							when 'PROGRESSUPDATE' then 'START'
							when 'REPLAN' then 'START'
							when 'RELEASEHOLD' then 'START'
							when 'REASSIGN' then 'START'
							when 'JSA Form' then 'jsa_form'
							when 'EICL Form' then 'eicl_form'
							when 'Field Inspection Form' then 'field_inspection_form'
							when 'IG Visit Form' then 'ig_visit_form'
							when 'Commissioning Form' then 'comm_form'
							when 'Commissioning FSR Form' then 'comm_fsr_form'
							when 'Sales Flow Form' then 'sales_flow_form'
							when 'Water Test Report' then 'water_test_form'
							when 'Thermax - Service Report' then 'service_report_thermax_form'
							when 'Sterling - Service Report' then 'service_report_sterling_form'
							when 'AII Report' then 'AII_report_acopco_form'
							when 'AIP Report' then 'AIP_report_acopco_form'
							when 'SPM Check Report' then 'SPM_check_report_form'
							else csel.eventverb_id
							end)
						from call_status_event_log csel
						where csel.company_id = @i_client_id
							and csel.country_code = @i_country_code
							and csel.call_ref_no = a.call_ref_no
							and csel.eventverb_id != 'Field Service Form'
						order by convert(datetime, csel.event_date) desc, csel.event_id desc
					)), '')	 + '",' +
			'"last_updated_time":"' + convert(varchar(10), SYSDATETIME(), 105) + ' ' + substring(CONVERT(varchar(10),SYSDATETIME(), 108), 1, 5) + '",' +
			isnull((case(select isnull(max(trip_seqno), '')
					from trip_sheet
					where company_id = @i_client_id
					  and country_code = @i_country_code
					  and trip_for_ind = 'C'
					  and isnull(employee_id,'') = @p_assigned_to_empid
					  and call_ref_no = a.call_ref_no
					  and trip_finish_datetime is null)
			  when '' then '"trip_start_date":"",'+
					'"trip_start_hour":"",'+
					'"trip_start_minute":"",'+
					'"trip_start_lat":"",' +
					'"trip_start_long":"",' +
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",'+
					'"trip_finish_lat":"",' +
					'"trip_finish_long":"",'
			  else (select top(1) '"trip_start_date":"'+ isnull(CONVERT(varchar(10), trip_start_datetime,121),'') +'",'+
					'"trip_start_hour":"'+ isnull(substring(CONVERT(varchar(10), trip_start_datetime, 108),1,2),'') +'",'+
					'"trip_start_minute":"'+ isnull(substring(CONVERT(varchar(10), trip_start_datetime, 108),4,2),'') +'",'+
					'"trip_start_lat":"' + isnull(start_lattitude_value, '') + '",' +
					'"trip_start_long":"' + isnull(start_longitude_value, '') + '",' +
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",'+
					'"trip_finish_lat":"",' +
					'"trip_finish_long":"",'
					from trip_sheet
					where company_id = @i_client_id
					  and country_code = @i_country_code
					  and trip_for_ind = 'C'
					  and isnull(employee_id,'') = @p_assigned_to_empid
					  and call_ref_no = a.call_ref_no
					  order by trip_seqno desc)
			 end), '"trip_start_date":"",'+
					'"trip_start_hour":"",'+
					'"trip_start_minute":"",'+
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",') +
			(case @o_udf_char_1 when 1 then '"udf_char_1":"'+ISNULL(udf_char_1,'')+'",' else '' end)+
			(case @o_udf_char_2 when 1 then '"udf_char_2":"'+ISNULL(udf_char_2,'')+'",' else '' end)+
			(case @o_udf_char_3 when 1 then '"udf_char_3":"'+ISNULL(udf_char_3,'')+'",' else '' end)+
			(case @o_udf_char_4 when 1 then '"udf_char_4":"'+ISNULL(udf_char_4,'')+'",' else '' end)+
			(case @o_udf_bit_1 when 1 then '"udf_bit_1":"'+cast(ISNULL(udf_bit_1,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_2 when 1 then '"udf_bit_2":"'+cast(ISNULL(udf_bit_2,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_3 when 1 then '"udf_bit_3":"'+cast(ISNULL(udf_bit_3,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_4 when 1 then '"udf_bit_4":"'+cast(ISNULL(udf_bit_4,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_float_1 when 1 then '"udf_float_1":"'+cast(ISNULL(udf_float_1,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_2 when 1 then '"udf_float_2":"'+cast(ISNULL(udf_float_2,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_3 when 1 then '"udf_float_3":"'+cast(ISNULL(udf_float_3,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_4 when 1 then '"udf_float_4":"'+cast(ISNULL(udf_float_4,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_date_1 when 1 then 
			'"udf_date_1":"'+isnull(convert(varchar(10), a.udf_date_1, 120),'')+'",'+
			'"udf_date_1_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),1,2),'')+'",'+
			'"udf_date_1_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_2 when 1 then 
			'"udf_date_2":"'+isnull(convert(varchar(10), a.udf_date_2, 120),'')+'",'+
			'"udf_date_2_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),1,2),'')+'",'+
			'"udf_date_2_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_3 when 1 then
			'"udf_date_3":"'+isnull(convert(varchar(10), a.udf_date_3, 120),'')+'",'+
			'"udf_date_3_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),1,2),'')+'",'+
			'"udf_date_3_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_4 when 1 then 
			'"udf_date_4":"'+isnull(convert(varchar(10), a.udf_date_4, 120),'')+'",'+
			'"udf_date_4_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),1,2),'')+'",'+
			'"udf_date_4_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),4,2),'')+'",' 
			else '' end)+ 
			(case @o_udf_analysis_code1 when 1 then '"udf_analysis_code1":"'+ISNULL(udf_analysis_code1,'')+'",' else '' end)+
			(case @o_udf_analysis_code2 when 1 then '"udf_analysis_code2":"'+ISNULL(udf_analysis_code2,'')+'",' else '' end)+
			(case @o_udf_analysis_code3 when 1 then '"udf_analysis_code3":"'+ISNULL(udf_analysis_code3,'')+'",' else '' end)+
			(case @o_udf_analysis_code4 when 1 then '"udf_analysis_code4":"'+ISNULL(udf_analysis_code4,'')+'",' else '' end)+    
			'"rec_tstamp":"'+cast(convert(uniqueidentifier,cast(a.last_update_timestamp as binary)) as varchar(36))+'",'
		as o_call_xml
	  from call_register a
	  where a.company_id = @i_client_id
		and a.country_code = @i_country_code
		and ( a.call_ref_no in 
					( select distinct x.call_ref_no 
					  from call_assignment x
					  where x.company_id = @i_client_id
						and x.country_code = @i_country_code
						and x.resource_emp_id = @p_assigned_to_empid
						and x.primary_resource_ind = 1
					 )
			)
			and (act_finish_date is null
			 or
			 DATEDIFF(DD, a.act_finish_date, SYSDATETIMEOFFSET()) = 0
			 )	
		
	union

		select  a.call_ref_no, 
			a.call_wf_stage_no, a.call_status,
			isnull(a.customer_id,''), isnull(a.customer_location_code,''),
			isnull(a.created_on_date,''), isnull(a.sch_start_date,''),
			isnull(a.sch_finish_date,''), isnull(a.act_start_date,''),
			isnull(a.act_finish_date,''), isnull(a.closed_on_date,''), 
		'{' +
			'"cust_location_code":"'+a.customer_location_code+'",'+
			'"call_no":"'+isnull(a.call_ref_no,'')+'",'+
			'"call_category":"'+a.call_category+'",'+
			'"call_category_desc":"'+
				case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLCATG'
						  and f.code = a.call_category)
					when 1 then
					(select isnull(e.short_description,'') 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLCATG'
					  and e.code = a.call_category)
					else
					(select isnull(g.short_description,'') from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLCATG'
					  and g.code = a.call_category)
				 end +
				'",'+
			'"call_type":"'+a.call_type+'",'+ 
			'"call_type_desc":"'+
				isnull(case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLTYPE'
						  and f.code = a.call_type)
					when 1 then
					(select isnull(e.short_description,'') 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLTYPE'
					  and e.code = a.call_type)
					else
					(select isnull(g.short_description,'') from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLTYPE'
					  and g.code = a.call_type)
				 end,'') +
				'",'+
			'"call_wf_stage":"'+convert(varchar(2), a.call_wf_stage_no)+'",'+
			'"call_wf_stage_desc":"'+
		   (
			 isnull((select isnull(x.description,'') 
			 from workflow_stage_master x
			 where x.company_id = @i_client_id
			   and x.country_code = @i_country_code
			   and x.transaction_type_code = 'CALL'
			   and x.request_category = a.call_category
			   and x.workflow_stage_no = a.call_wf_stage_no),'')
			)
		   +'",'+ 
			'"call_status":"'+a.call_status+'",'+
			'"call_status_desc":"'+
				case (select 1 from code_table_mlingual_translation f
				where f.company_id = @i_client_id
					and f.country_code = @i_country_code
					and f.locale_id = @i_locale_id
					and f.code_type = 'CALLSTATUS'
					and f.code = a.call_status)
				when 1 then
				(select e.long_description 
				from code_table_mlingual_translation e
				where e.company_id = @i_client_id
					and e.country_code = @i_country_code
					and e.locale_id = @i_locale_id
					and e.code_type = 'CALLSTATUS'
					and e.code = a.call_status)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
					and g.country_code = @i_country_code
					and g.locale_id = 'ALL'
					and g.code_type = 'CALLSTATUS'
					and g.code = a.call_status)
				end + '",'+
			'"priority_cd":"'+a.priority_code+'",'+
			'"company_location_code":"'+ a.company_location_code+'",'+
			'"asset_id":"'+a.asset_id+	'",'+
			'"active_call":"'+ 
				case (select 1 
				from employee_lastaccess_info b
				where a.company_id = b.company_id
					and a.country_code = b.country_code
					and isnull(b.employee_id,'') = @p_assigned_to_empid
					and b.last_accessed_txn_ref_no = a.call_ref_no
					and b.allow_newtxn_ind = 0
				)
				when 1 then 'T'
				else 'F'
				end + '",' + 
			'"problem_description":"'+isnull(a.problem_description,'')+'",'+
			'"No_of_trips":"'+cast(isnull((select count(*) from trip_sheet ts
				where ts.company_id = @i_client_id
					and ts.country_code = @i_country_code
					and ts.trip_for_ind = 'C'
					and isnull(ts.employee_id,'') = @p_assigned_to_empid
					and ts.call_ref_no = a.call_ref_no ),0) as varchar(4))+'",'+
			'"total_trip_distance_in_kms":"'+cast(isnull((select SUM(distance_in_kms) from trip_sheet ts1
				where ts1.company_id = @i_client_id
					and ts1.country_code = @i_country_code
					and ts1.trip_for_ind = 'C'
					and isnull(ts1.employee_id,'') = @p_assigned_to_empid
					and ts1.call_ref_no = a.call_ref_no ),0) as varchar(66))+'",'+
			'"total_travel_time_in_hrs":"'+cast(isnull((select sum(DATEdiff(hh, ts2.trip_finish_datetime, ts2.trip_start_datetime))
				from trip_sheet ts2
				where ts2.company_id = @i_client_id
					and ts2.country_code = @i_country_code
					and ts2.trip_for_ind = 'C'
					and isnull(ts2.employee_id,'') = @p_assigned_to_empid
					and ts2.call_ref_no = a.call_ref_no ),0) as varchar(6))+'",'+
			'"total_effort_in_hrs":"'+cast(isnull((
				select SUM(datediff(hh, crul.to_date, crul.from_date))
				from call_resource_utilisation_log crul
				where crul.company_id = @i_client_id
				and crul.country_code = @i_country_code
				and crul.call_ref_no = a.call_ref_no
				and isnull(crul.resource_emp_id,'') = @p_assigned_to_empid
				),0) as varchar(6))+'",'+
			'"addn_desc":"'+ISNULL(a.additional_information,'')+'",'+ /* this may overflow */
			'"company_location_name":"'+
			   isnull((select cl2.location_name_short 
						from company_location cl2
						where cl2.company_id = @i_client_id
						  and cl2.country_code = @i_country_code
						  and cl2.location_code = a.company_location_code),'')
			   +'",'+
			'"cust_contact_name":"'+isnull(a.customer_contact_name,'')+'",'+
			'"cust_contact_no":"'+isnull(a.customer_contact_no,'')+'",'+
			'"cust_contact_email_id":"'+isnull(a.customer_contact_email_id,'')+'",'+
			'"customer_id":"'+ISNULL(a.customer_id,'')+'",'+
			'"asset_loc_reported":"'+isnull(a.asset_location_code_reported,'')+'",'+
			'"equipment_id":"'+ISNULL(a.equipment_id,'')+'",'+
			'"created_on_date":"'+isnull(CONVERT(varchar(10),a.created_on_date,120),'')+'",'+
			'"created_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.created_on_date,108),1,2),'')+'",'+
			'"created_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.created_on_date,108),4,2),'')+'",'+
			'"closed_on_date":"'+isnull(CONVERT(varchar(10),a.closed_on_date,120),'')+'",'+
			'"closed_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.closed_on_date,108),1,2),'')+'",'+
			'"closed_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.closed_on_date,108),4,2),'')+'",'+       
			'"sch_start_on_date":"'+isnull(CONVERT(varchar(10),a.sch_start_date,120),'')+'",'+
			'"sch_start_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.sch_start_date,108),1,2),'')+'",'+
			'"sch_start_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.sch_start_date,108),4,2),'')+'",'+       
			'"sch_finish_on_date":"'+isnull(CONVERT(varchar(10),a.sch_finish_date,120),'')+'",'+
			'"sch_finish_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.sch_finish_date,108),1,2),'')+'",'+
			'"sch_finish_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.sch_finish_date,108),4,2),'')+'",'+       
			'"plan_duration":"'+isnull(CAST(a.plan_duration as varchar(8)),'')+'",'+
			'"plan_duration_uom":"'+isnull(CAST(a.plan_duration_uom as varchar(3)),'')+'",'+
			'"plan_duration_uom_desc":"'+ISNULL(
			case (select 1 from code_table_mlingual_translation f
					where f.company_id = @i_client_id
					  and f.country_code = @i_country_code
					  and f.locale_id = @i_locale_id
					  and f.code_type = 'TDURUOM'
					  and f.code = a.plan_duration_uom)
				when 1 then
				(select e.long_description 
					from code_table_mlingual_translation e
				where e.company_id = @i_client_id
				  and e.country_code = @i_country_code
				  and e.locale_id = @i_locale_id
				  and e.code_type = 'TDURUOM'
				  and e.code = a.plan_duration_uom)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
				  and g.country_code = @i_country_code
				  and g.locale_id = 'ALL'
				  and g.code_type = 'TDURUOM'
				  and g.code = a.plan_duration_uom)
			 end,'') +
			'",'+
			'"plan_work":"'+isnull(CAST(a.plan_work as varchar(8)),'')+'",'+
			'"plan_work_uom":"'+isnull(CAST(a.plan_work_uom as varchar(3)),'')+'",'+
			'"plan_work_uom_desc":"'+ ISNULL(
			case (select 1 from code_table_mlingual_translation f
					where f.company_id = @i_client_id
					  and f.country_code = @i_country_code
					  and f.locale_id = @i_locale_id
					  and f.code_type = 'TWORKUOM'
					  and f.code = a.plan_work_uom)
				when 1 then
				(select e.long_description 
					from code_table_mlingual_translation e
				where e.company_id = @i_client_id
				  and e.country_code = @i_country_code
				  and e.locale_id = @i_locale_id
				  and e.code_type = 'TWORKUOM'
				  and e.code = a.plan_work_uom)
				else
				(select g.long_description from code_table_mlingual_translation g
				where g.company_id = @i_client_id
				  and g.country_code = @i_country_code
				  and g.locale_id = 'ALL'
				  and g.code_type = 'TWORKUOM'
				  and g.code = a.plan_work_uom)
			 end,'') +
			'",'+
			'"act_start_on_date":"'+isnull(CONVERT(varchar(10),a.act_start_date,120),'')+'",'+
			'"act_start_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),1,2),'')+'",'+
			'"act_start_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),4,2),'')+'",'+       
			'"act_finish_on_date":"'+isnull(CONVERT(varchar(10),a.act_finish_date,120),'')+'",'+
			'"act_finish_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),1,2),'')+'",'+
			'"act_finish_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),4,2),'')+'",'+      
			'"act_duration":"'+isnull(CAST(a.actual_duration as varchar(8)),'')+'",'+
			'"act_work":"'+isnull(CAST(a.actual_work as varchar(8)),'')+'",'+
			'"head_emp_name":"'+
			ISNULL((select e1.title+'.'+e1.first_name+ISNULL(e1.middle_name,'')+e1.last_name
					from employee e1 JOIN company_location cl
					ON e1.employee_id = cl.head_emp_id
					where e1.company_id = @i_client_id
					and e1.country_code = @i_country_code
					and cl.company_id = e1.company_id
					and cl.country_code = e1.country_code
					and cl.location_code = a.company_location_code),'')
			+'",'+
			'"head_emp_contact":"'+
			isnull((select e1.contact_mobile_no
					from employee e1 JOIN company_location cl
					ON e1.employee_id = cl.head_emp_id
					where e1.company_id = @i_client_id
					and e1.country_code = @i_country_code
					and cl.company_id = e1.company_id
					and cl.country_code = e1.country_code
					and cl.location_code = a.company_location_code ),'')
			+'",'+
			'"last_accessed_feature":"' + isnull((
				select ftr.feature_id   
				from company_feature ftr
				where ftr.company_id = @i_client_id
					and ftr.country_code = @i_country_code
					and ftr.channel_id = 'Mobile'
					and ftr.screen_id = (
						select top(1) (case(csel.eventverb_id)
							when null then ''
							when 'Trip start' then 'trip_start'
							when 'Trip finish' then 'trip_finish'
							when 'PROGRESSUPDATE' then 'START'
							when 'REPLAN' then 'START'
							when 'RELEASEHOLD' then 'START'
							when 'REASSIGN' then 'START'
							when 'JSA Form' then 'jsa_form'
							when 'EICL Form' then 'eicl_form'
							when 'Field Inspection Form' then 'field_inspection_form'
							when 'IG Visit Form' then 'ig_visit_form'
							when 'Commissioning Form' then 'comm_form'
							when 'Commissioning FSR Form' then 'comm_fsr_form'
							when 'Sales Flow Form' then 'sales_flow_form'
							when 'Water Test Report' then 'water_test_form'
							when 'Thermax - Service Report' then 'service_report_thermax_form'
							when 'Sterling - Service Report' then 'service_report_sterling_form'
							when 'AII Report' then 'AII_report_acopco_form'
							when 'AIP Report' then 'AIP_report_acopco_form'
							when 'SPM Check Report' then 'SPM_check_report_form'
							else csel.eventverb_id
							end)
						from call_status_event_log csel
						where csel.company_id = @i_client_id
							and csel.country_code = @i_country_code
							and csel.call_ref_no = a.call_ref_no
							and csel.eventverb_id != 'Field Service Form'
						order by convert(datetime, csel.event_date) desc, csel.event_id desc
					)), '')	 + '",' +
			'"last_updated_time":"' + convert(varchar(10), SYSDATETIME(), 105) + ' ' + substring(CONVERT(varchar(10),SYSDATETIME(), 108), 1, 5) + '",' +
			isnull((case(select isnull(max(trip_seqno), '')
					from trip_sheet
					where company_id = @i_client_id
					  and country_code = @i_country_code
					  and trip_for_ind = 'C'
					  and isnull(employee_id,'') = @p_assigned_to_empid
					  and call_ref_no = a.call_ref_no
					  and trip_finish_datetime is null)
			  when '' then '"trip_start_date":"",'+
					'"trip_start_hour":"",'+
					'"trip_start_minute":"",'+
					'"trip_start_lat":"",' +
					'"trip_start_long":"",' +
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",'+
					'"trip_finish_lat":"",' +
					'"trip_finish_long":"",'
			  else (select top(1) '"trip_start_date":"'+ isnull(CONVERT(varchar(10), trip_start_datetime,121),'') +'",'+
					'"trip_start_hour":"'+ isnull(substring(CONVERT(varchar(10), trip_start_datetime, 108),1,2),'') +'",'+
					'"trip_start_minute":"'+ isnull(substring(CONVERT(varchar(10), trip_start_datetime, 108),4,2),'') +'",'+
					'"trip_start_lat":"' + isnull(start_lattitude_value, '') + '",' +
					'"trip_start_long":"' + isnull(start_longitude_value, '') + '",' +
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",'+
					'"trip_finish_lat":"",' +
					'"trip_finish_long":"",'
					from trip_sheet
					where company_id = @i_client_id
					  and country_code = @i_country_code
					  and trip_for_ind = 'C'
					  and isnull(employee_id,'') = @p_assigned_to_empid
					  and call_ref_no = a.call_ref_no
					  order by trip_seqno desc)
			 end), '"trip_start_date":"",'+
					'"trip_start_hour":"",'+
					'"trip_start_minute":"",'+
					'"trip_finish_date":"",'+
					'"trip_finish_hour":"",'+
					'"trip_finish_minute":"",') +
			(case @o_udf_char_1 when 1 then '"udf_char_1":"'+ISNULL(udf_char_1,'')+'",' else '' end)+
			(case @o_udf_char_2 when 1 then '"udf_char_2":"'+ISNULL(udf_char_2,'')+'",' else '' end)+
			(case @o_udf_char_3 when 1 then '"udf_char_3":"'+ISNULL(udf_char_3,'')+'",' else '' end)+
			(case @o_udf_char_4 when 1 then '"udf_char_4":"'+ISNULL(udf_char_4,'')+'",' else '' end)+
			(case @o_udf_bit_1 when 1 then '"udf_bit_1":"'+cast(ISNULL(udf_bit_1,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_2 when 1 then '"udf_bit_2":"'+cast(ISNULL(udf_bit_2,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_3 when 1 then '"udf_bit_3":"'+cast(ISNULL(udf_bit_3,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_bit_4 when 1 then '"udf_bit_4":"'+cast(ISNULL(udf_bit_4,0) as varchar(1))+'",' else '' end)+
			(case @o_udf_float_1 when 1 then '"udf_float_1":"'+cast(ISNULL(udf_float_1,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_2 when 1 then '"udf_float_2":"'+cast(ISNULL(udf_float_2,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_3 when 1 then '"udf_float_3":"'+cast(ISNULL(udf_float_3,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_float_4 when 1 then '"udf_float_4":"'+cast(ISNULL(udf_float_4,0) as varchar(14))+'",' else '' end)+
			(case @o_udf_date_1 when 1 then 
			'"udf_date_1":"'+isnull(convert(varchar(10), a.udf_date_1, 120),'')+'",'+
			'"udf_date_1_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),1,2),'')+'",'+
			'"udf_date_1_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_2 when 1 then 
			'"udf_date_2":"'+isnull(convert(varchar(10), a.udf_date_2, 120),'')+'",'+
			'"udf_date_2_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),1,2),'')+'",'+
			'"udf_date_2_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_3 when 1 then
			'"udf_date_3":"'+isnull(convert(varchar(10), a.udf_date_3, 120),'')+'",'+
			'"udf_date_3_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),1,2),'')+'",'+
			'"udf_date_3_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),4,2),'')+'",'       
			else '' end)+
			(case @o_udf_date_4 when 1 then 
			'"udf_date_4":"'+isnull(convert(varchar(10), a.udf_date_4, 120),'')+'",'+
			'"udf_date_4_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),1,2),'')+'",'+
			'"udf_date_4_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),4,2),'')+'",' 
			else '' end)+ 
			(case @o_udf_analysis_code1 when 1 then '"udf_analysis_code1":"'+ISNULL(udf_analysis_code1,'')+'",' else '' end)+
			(case @o_udf_analysis_code2 when 1 then '"udf_analysis_code2":"'+ISNULL(udf_analysis_code2,'')+'",' else '' end)+
			(case @o_udf_analysis_code3 when 1 then '"udf_analysis_code3":"'+ISNULL(udf_analysis_code3,'')+'",' else '' end)+
			(case @o_udf_analysis_code4 when 1 then '"udf_analysis_code4":"'+ISNULL(udf_analysis_code4,'')+'",' else '' end)+    
			'"rec_tstamp":"'+cast(convert(uniqueidentifier,cast(a.last_update_timestamp as binary)) as varchar(36))+'",'
		as o_call_xml
	  from call_register a
	  where a.company_id = @i_client_id
		and a.country_code = @i_country_code
		and ( a.call_ref_no in 
					( select distinct b. last_accessed_txn_ref_no
					  from employee_lastaccess_info b
					  where b.company_id = @i_client_id
						and b.country_code = @i_country_code
						and b.employee_id = @p_assigned_to_empid
						and b.last_accessed_txn_ind = 'C'
						and b.allow_newtxn_ind = 0
					 )
			)
	
	if @@ROWCOUNT != 0
	begin
	
		update #call_list
		set call_assigned_to_emp_id = b.resource_emp_id,
			call_assigned_to_emp_name =  c.title+' '+c.first_name+' '+c.last_name,
			assigned_on_date = b.assigned_on_date
		from call_assignment b, employee c
		where b.company_id = @i_client_id
		  and b.country_code = @i_country_code
		  and b.call_ref_no = #call_list.call_ref_no
		  and b.resource_emp_id = c.employee_id
		  and b.primary_resource_ind = 1
  
		/* Pick the event record and calculate delay_in_hrs_same_status */
		update #call_list
		set delay_in_hrs_same_status = DATEDIFF(hh, convert(datetime,b.event_date),convert(datetime,SYSDATETIMEOFFSET())) 
		from call_status_event_log b
		where b.company_id = @i_client_id
		  and b.country_code = @i_country_code
		  and b.call_ref_no = #call_list.call_ref_no
		  and b.to_wf_stage_no = #call_list.call_wf_stage_no
		  and b.to_status = #call_list.call_status
		  and DATEDIFF(second,b.event_date, 
				(
					select max(f.event_date) from call_status_event_log f 
					where f.company_id = @i_client_id
					  and f.country_code = @i_country_code
					  and f.call_ref_no = #call_list.call_ref_no
					  and f.to_wf_stage_no = #call_list.call_wf_stage_no
					  and f.to_status = #call_list.call_status
					)) = 0
		
	
	update #call_list
	set customer_name = b.customer_name
	from customer b
	where isnull(#call_list.customer_id,'') not in ( '','ZZZ')
	and b.company_id = @i_client_id
	and b.country_code = @i_country_code
	and #call_list.customer_id = b.customer_id
	  
	  
	select @p_checksum_ind = isnull(convert(varchar(15), assigned_activity_checksum_value), '')
	from employee_lastaccess_info
	where company_id = @i_client_id
		and country_code = @i_country_code
		and employee_id = @p_assigned_to_empid
	
	update employee_lastaccess_info
	set channel_refresh_indicator = 0,
		checksum_updated_by_emp_id = @p_assigned_to_empid,
		checksum_updated_channel_id = 'MOBILE'
	where company_id = @i_client_id
		and country_code = @i_country_code
		and employee_id = @p_assigned_to_empid

	select top(100) '' as call_list,			
		call_xml+			
		'"assigned_to_emp_id":"'+isnull(call_assigned_to_emp_id,'')+'",'+
		'"assigned_to_emp_name":"'+ISNULL(call_assigned_to_emp_name,'')+'",'+
		'"assigned_on_date":"'+isnull(CONVERT(varchar(10),assigned_on_date,120),'')+'",'+
		'"assigned_on_hour":"'+isnull(substring(CONVERT(varchar(10),assigned_on_date,108),1,2),'')+'",'+
		'"assigned_on_minute":"'+isnull(substring(CONVERT(varchar(10),assigned_on_date,108),4,2),'')+'",'+
		'"customer_name":"'+ISNULL(customer_name,'')+'",'+
		'"delay_in_hrs":"'+cast(ISNULL(delay_in_hrs_same_status,0) as varchar(10))+'",' +
		'"checksumVal":"' + isnull(@p_checksum_ind, '') + '"' +
	'}' as o_call_xml
	from #call_list	  
	order by created_on_date desc

    	  set @o_retrieve_status = ''
	end
    else 
    begin
		select '' as call_list, 
				'' as o_call_xml
		where 1=2
		set @o_retrieve_status = 'SP001'
	end


    SET NOCOUNT OFF;
END

