
create PROCEDURE [dbo].[sp_retrieve_manage_call_register_details] 
    @i_client_id [uddt_client_id], 
    @i_country_code [uddt_country_code], 
    @i_session_id [sessionid], 
    @i_user_id [userid], 
    @i_locale_id [uddt_locale_id], 
    @i_call_ref_no [uddt_nvarchar_20], 
    @o_retrieve_status [uddt_varchar_5] OUTPUT
AS
BEGIN
    /*
     * Retrieves Service call details - Manage Service Call List
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
    
    -- Result set 1: call_detail

    SELECT
        '' as call_detail, /* dummy column aliased by result set name */
        '' as o_call_detail_json /* unicode string */
    FROM <Table name>
    
   */

	declare @p_request_category varchar(10)
	
	select @o_retrieve_status = ''

	select @p_request_category = call_category
	from call_register
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and call_ref_no = @i_call_ref_no
	  
	create table #call_details
    (
		call_ref_no nvarchar(20) not null,
		call_status varchar(2) not null,
		call_xml nvarchar(max) not null,
		call_closed_on_date datetimeoffset(7) null,
		assigned_to_emp_id nvarchar(12) null,
		assigned_to_emp_name nvarchar(100) null,
		assigned_on_date datetimeoffset(7) null,
		attachment_reference nvarchar(255) null,
		attachment_doc_no nvarchar(60) null,
		asset_id nvarchar(30) null,
		service_contract_doc_no nvarchar(40) null,
		service_visit_slno tinyint null 
    )
	insert #call_details
	(
		call_ref_no, asset_id, call_status, call_closed_on_date,
		service_contract_doc_no,call_xml
	)
	select a.call_ref_no, a.asset_id, a.call_status,  a.closed_on_date,
			isnull(a.service_contract_doc_no,''), 
			'{' +
			'"cust_id":"'+ a.customer_id+'",'+
			'"cust_name":"'+ 
				isnull((select customer_name
				 from customer cx1
				 where cx1.company_id = @i_client_id
				   and cx1.country_code = @i_country_code
				   and cx1.customer_id = a.customer_id  
				),'')
			+'",'+
			'"call_no":"'+a.call_ref_no+'",'+
			'"call_category":"'++a.call_category+'",'+
			'"call_category_desc":"'+
				case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLCATG'
						  and f.code = a.call_category)
					when 1 then
					(select e.short_description 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLCATG'
					  and e.code = a.call_category)
					else
					(select g.short_description from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLCATG'
					  and g.code = a.call_category)
				 end +
				'",'+
			'"call_type":"'+a.call_type+'",'+
			'"call_type_desc":"'+
				case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLTYPE'
						  and f.code = a.call_type)
					when 1 then
					(select e.short_description 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLTYPE'
					  and e.code = a.call_type)
					else
					(select g.short_description from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLTYPE'
					  and g.code = a.call_type)
				 end +
				'",'+
			'"prob_desc":"'+isnull(a.problem_description,'')+'",'+
			'"addn_desc":"'+ISNULL(a.additional_information,'')+'",'+
			'"call_wf_stage":"'+
			   (
				 select x.description 
				 from workflow_stage_master x
				 where x.company_id = @i_client_id
				   and x.country_code = @i_country_code
				   and x.transaction_type_code = 'CALL'
				   and x.request_category = @p_request_category
				   and x.workflow_stage_no = a.call_wf_stage_no
				)
			   +'",'+
			   '"call_status":"'+  a.call_status+'",'+
			   '"call_status_desc":"'+
				case (select 1 from code_table_mlingual_translation f
						where f.company_id = @i_client_id
						  and f.country_code = @i_country_code
						  and f.locale_id = @i_locale_id
						  and f.code_type = 'CALLSTATUS'
						  and f.code = a.call_status)
					when 1 then
					(select e.short_description 
						from code_table_mlingual_translation e
					where e.company_id = @i_client_id
					  and e.country_code = @i_country_code
					  and e.locale_id = @i_locale_id
					  and e.code_type = 'CALLSTATUS'
					  and e.code = a.call_status)
					else
					(select g.short_description from code_table_mlingual_translation g
					where g.company_id = @i_client_id
					  and g.country_code = @i_country_code
					  and g.locale_id = 'ALL'
					  and g.code_type = 'CALLSTATUS'
					  and g.code = a.call_status)
				 end +
				'",'+
			   '"priority_cd":"'+a.priority_code+'",'+
			   '"equipment_id":"'+isnull(a.equipment_id, '')+'",'+	
			   '"equipment_desc":"'+
					   isnull((select ex1.description from equipment ex1
					   where ex1.company_id = @i_client_id
						 and ex1.country_code = @i_country_code
						 and ex1.equipment_id = a.equipment_id
						),'')
			   +'",'+	
			    '"asset_in_warranty_ind":"'+cast(ISNULL(asset_in_warranty_ind,0) as varchar(1))+'",'+
                '"billable_nonbillable_ind":"'+billable_nonbillable_ind+'",'+
				'"billable_nonbillable_ind_desc":"'+
					ISNULL((''), '')
				+'",'+
				'"charges_currency_code":"'+charges_currency_code+'",'+
				'"charges_currency_code_desc":"'+'INR'+'",'+
				'"charges_gross_amount":"'+cast(charges_gross_amount as varchar(14))+'",'+
				'"charges_discount_amount":"'+cast(charges_discount_amount as varchar(14))+'",'+
				'"charges_tax_amount":"'+cast(charges_tax_amount as varchar(14))+'",'+
				'"charges_net_amount":"'+cast(charges_net_amount as varchar(14))+'",'+
			   '"cust_location_code":"'+isnull(a.customer_location_code,'')+'",'+
				'"cust_location_name":"'+ 
				isnull((select location_name_short
				 from customer_location cl1
				 where cl1.company_id = @i_client_id
				   and cl1.country_code = @i_country_code
				   and cl1.customer_id = a.customer_id
				   and cl1.location_code = a.customer_location_code  
					),'')
				+'",'+
			   '"cust_contact_name":"'+isnull(a.customer_contact_name,'')+'",'+
			   '"cust_contact_no":"'+isnull(a.customer_contact_no,'')+'",'+
			   '"cust_contact_email_id":"'+isnull(a.customer_contact_email_id,'')+'",'+
			   '"company_location_code":"'+isnull(a.company_location_code,'')+'",'+
			   '"company_location_name":"'+
			   isnull((select cl2.location_name_short 
						from company_location cl2
						where cl2.company_id = @i_client_id
						  and cl2.country_code = @i_country_code
						  and cl2.location_code = a.company_location_code),'')
			   +'",'+
			   '"asset_id":"'+isnull(a.asset_id,'')+'",'+
			   '"asset_desc":"'+
					   isnull((select ex1.description from equipment ex1
					   where ex1.company_id = @i_client_id
						 and ex1.country_code = @i_country_code
						 and ex1.equipment_id = a.equipment_id
						),'')
			   +'",'+	
			   '"asset_loc_reported":"'+isnull(a.asset_location_code_reported,'')+'",'+
			   '"created_on_date":"'+CONVERT(varchar(10),a.created_on_date,120)+'",'+
			   '"created_on_hour":"'+substring(CONVERT(varchar(10),a.created_on_date,108),1,2)+'",'+
			   '"created_on_minute":"'+substring(CONVERT(varchar(10),a.created_on_date,108),4,2)+'",'+
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
			   '"plan_work":"'+isnull(CAST(a.plan_work as varchar(8)),'')+'",'+
			   '"plan_work_uom":"'+isnull(CAST(a.plan_work_uom as varchar(3)),'')+'",'+
			   '"act_start_on_date":"'+isnull(CONVERT(varchar(10),a.act_start_date,120),'')+'",'+
			   '"act_start_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),1,2),'')+'",'+
			   '"act_start_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_start_date,108),4,2),'')+'",'+       
			   '"act_finish_on_date":"'+isnull(CONVERT(varchar(10),a.act_finish_date,120),'')+'",'+
			   '"act_finish_on_hour":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),1,2),'')+'",'+
			   '"act_finish_on_minute":"'+isnull(substring(CONVERT(varchar(10),a.act_finish_date,108),4,2),'')+'",'+      
			   '"act_duration":"'+isnull(CAST(a.actual_duration as varchar(8)),'')+'",'+
			   '"act_work":"'+isnull(CAST(a.actual_work as varchar(8)),'')+'",'+
			   '"org_level_no":"'+cast(a.organogram_level_no as varchar(3))+'",'+
			   '"org_level_code":"'+a.organogram_level_code+'",'+
			   '"call_mapped_to_func_role":"'+isnull(a.call_mapped_to_func_role,'')+'",'+
			   '"call_mapped_func_role_desc":"'+
			     ISNULL(( select fr1.role_description
						  from functional_role fr1
						  where fr1.company_id = @i_client_id
						    and fr1.country_code = @i_country_code
						    and fr1.functional_role_id = a.call_mapped_to_func_role), '')
			   +'",'+
			   '"call_mapped_to_emp_id":"'+isnull(a.call_mapped_to_employee_id,'')+'",'+
			   '"call_mapped_to_emp_name":"'+
				ISNULL((select e1.title+'.'+e1.first_name+ISNULL(e1.middle_name,'')+e1.last_name
						from employee e1
						where e1.company_id = @i_client_id
						  and e1.country_code = @i_country_code
						  and e1.employee_id = a.call_mapped_to_employee_id ),'')
			   +'",'+
			   '"service_contract_doc_no":"'+ISNULL(service_contract_doc_no,'')+'",'+
			   '"service_contract_doc_desc":"'+
					isnull(( select asc1.description
							 from asset_service_contract asc1
							 where asc1.company_id = @i_client_id
							   and asc1.country_code = @i_country_code
							   and asc1.asset_id = a.asset_id
							   and asc1.contract_doc_no = a.service_contract_doc_no),'')
				+'",'+
			   '"udf_char_1":"'+ISNULL(a.udf_char_1,'')+'",'+
			   '"udf_char_2":"'+ISNULL(a.udf_char_2,'')+'",'+
			   '"udf_char_3":"'+ISNULL(a.udf_char_3,'')+'",'+
			   '"udf_char_4":"'+ISNULL(a.udf_char_4,'')+'",'+
			   '"udf_char_5":"'+ISNULL(a.udf_char_5,'')+'",'+
			   '"udf_char_6":"'+ISNULL(a.udf_char_6,'')+'",'+
			   '"udf_char_7":"'+ISNULL(a.udf_char_7,'')+'",'+
			   '"udf_char_8":"'+ISNULL(a.udf_char_8,'')+'",'+
			   '"udf_char_9":"'+ISNULL(a.udf_char_9,'')+'",'+
			   '"udf_char_10":"'+ISNULL(a.udf_char_10,'')+'",'+
			   '"udf_bit_1":"'+cast(ISNULL(a.udf_bit_1,0) as varchar(1))+'",'+
			   '"udf_bit_2":"'+cast(ISNULL(a.udf_bit_2,0) as varchar(1))+'",'+
			   '"udf_bit_3":"'+cast(ISNULL(a.udf_bit_3,0) as varchar(1))+'",'+
			   '"udf_bit_4":"'+cast(ISNULL(a.udf_bit_4,0) as varchar(1))+'",'+
			   '"udf_float_1":"'+CAST(isnull(cast(udf_float_1 as numeric(14,2)),0) as varchar(14))+'",'+
			   '"udf_float_2":"'+CAST(isnull(cast(udf_float_2 as numeric(14,2)),0) as varchar(14))+'",'+
			   '"udf_float_3":"'+CAST(isnull(cast(udf_float_3 as numeric(14,2)),0) as varchar(14))+'",'+
			   '"udf_float_4":"'+CAST(isnull(cast(udf_float_4 as numeric(14,2)),0) as varchar(14))+'",'+
			   '"udf_date_1":"'+isnull(convert(varchar(10), a.udf_date_1, 120),'')+'",'+
			   '"udf_date_1_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),1,2),'')+'",'+
			   '"udf_date_1_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_1, 108),4,2),'')+'",'+
			   '"udf_date_2":"'+isnull(convert(varchar(10), a.udf_date_2, 120),'')+'",'+
			   '"udf_date_2_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),1,2),'')+'",'+
			   '"udf_date_2_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_2, 108),4,2),'')+'",'+
			   '"udf_date_3":"'+isnull(convert(varchar(10), a.udf_date_3, 120),'')+'",'+
			   '"udf_date_3_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),1,2),'')+'",'+
			   '"udf_date_3_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_3, 108),4,2),'')+'",'+
			   '"udf_date_4":"'+isnull(convert(varchar(10), a.udf_date_4, 120),'')+'",'+
			   '"udf_date_4_hour":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),1,2),'')+'",'+
			   '"udf_date_4_minute":"'+isnull(substring(CONVERT(varchar(10), a.udf_date_4, 108),4,2),'')+'",'+
			   '"udf_analysis_code1":"'+isnull(a.udf_analysis_code1,'')+'",'+
			   '"udf_analysis_code2":"'+isnull(a.udf_analysis_code2,'')+'",'+
			   '"udf_analysis_code3":"'+isnull(a.udf_analysis_code3,'')+'",'+
			   '"udf_analysis_code4":"'+isnull(a.udf_analysis_code4,'')+'",'+
			   '"rec_tstamp":"'+cast(convert(uniqueidentifier,cast(a.last_update_timestamp as binary)) as varchar(36))+'",'
  from call_register a
  where a.company_id = @i_client_id
	and a.country_code = @i_country_code
	and a.call_ref_no = @i_call_ref_no


		update #call_details
		set assigned_to_emp_id = b.resource_emp_id,
			assigned_to_emp_name =  c.title+' '+c.first_name+' '+c.last_name,
			assigned_on_date = b.assigned_on_date
		from call_assignment b, employee c
		where b.company_id = @i_client_id
		  and b.country_code = @i_country_code
		  and b.call_ref_no = #call_details.call_ref_no
		  and b.company_id   = c.company_id
		  and b.country_code = c.country_code
		  and b.resource_emp_id = c.employee_id
		  and b.primary_resource_ind = 1	
	
		update #call_details
		set attachment_reference = b.attachment_file_name,
			attachment_doc_no = ''
		from call_user_attachments b
		where #call_details.call_closed_on_date is not null
		  and b.company_id = @i_client_id
		  and b.country_code = @i_country_code
		  and b.call_ref_no = #call_details.call_ref_no
		  and b.closure_report_ind = 1
		  
		update #call_details
		set service_visit_slno = b.service_visit_slno
		from asset_service_schedule b
		where b.company_id = @i_client_id
		  and b.country_code = @i_country_code
		  and b.asset_id = #call_details.asset_id
		  and b.call_jo_ind = 'C'
		  and b.call_ref_jo_no = #call_details.call_ref_no
		  and b.contract_doc_no = #call_details.service_contract_doc_no 

	select '' as call_detail,
			call_xml+
			'"assigned_to_emp_id":"'+ISNULL(assigned_to_emp_id,'')+'",'+
		   '"assigned_to_emp_name":"'+ISNULL(assigned_to_emp_name,'')+'",'+	
		   '"assigned_on_date":"'+isnull(CONVERT(varchar(10),assigned_on_date,120),'')+'",'+
		   '"assigned_on_hour":"'+isnull(substring(CONVERT(varchar(10),assigned_on_date,108),1,2),'')+'",'+
		   '"assigned_on_minute":"'+isnull(substring(CONVERT(varchar(10),assigned_on_date,108),4,2),'')+'",'+
		   '"attachment_reference":"'+ISNULL(attachment_reference,'')+'",'+
		   '"attachment_doc_no":"'+ISNULL(attachment_doc_no,'')+'",'+
		   '"service_visit_slno":"'+isnull(cast(service_visit_slno as varchar(3)),'')+'"'
		   +'}'
			as o_call_detail_json
	from #call_details

	set @o_retrieve_status = 'SP001'
	
    SET NOCOUNT OFF;
END
