

create PROCEDURE [dbo].[sp_save_manage_call_register] 
    @i_client_id [uddt_client_id], 
    @i_country_code [uddt_country_code], 
    @i_session_id [sessionid], 
    @i_user_id [userid], 
    @i_locale_id [uddt_locale_id], 
    @i_customer_id [uddt_customer_id], 
    @i_asset_id [uddt_asset_id], 
    @i_asset_location_reported [uddt_nvarchar_50], 
    @i_equipment_id [uddt_nvarchar_30], 
    @i_problem_description [uddt_nvarchar_1000], 
    @i_priority_code [uddt_varchar_3], 
    @i_call_logged_by_userid [userid], 
    @i_call_logged_on_date [uddt_date], 
    @i_call_logged_on_hour [uddt_hour], 
    @i_call_logged_on_minute [uddt_minute], 
    @i_customer_location_code [uddt_nvarchar_10], 
    @i_organogram_level_no [uddt_tinyint], 
    @i_organogram_level_code [uddt_nvarchar_15], 
    @i_call_category [uddt_varchar_10], 
    @i_call_type [uddt_varchar_10], 
    @i_additional_description [uddt_nvarchar_1000], 
    @i_company_location_code [uddt_nvarchar_8], 
    @i_customer_contact_name [uddt_nvarchar_60], 
    @i_customer_contact_no [uddt_nvarchar_20], 
    @i_customer_contact_email_id [uddt_nvarchar_60], 
    @i_billable_nonbillable_ind [uddt_varchar_2], 
    @i_charges_gross_amount [uddt_numeric_14_4], 
    @i_charges_discount_amount [uddt_numeric_14_4], 
    @i_charges_tax_amount [uddt_numeric_14_4], 
    @i_charges_net_amount [uddt_numeric_14_4], 
    @i_charges_currency_code [uddt_varchar_3], 
    @i_contract_doc_no [uddt_nvarchar_40], 
    @i_contract_visit_no [uddt_tinyint], 
    @i_call_mapped_to_func_role [uddt_nvarchar_30], 
    @i_call_mapped_to_employee_id [uddt_employee_id], 
    @i_call_udf_xml [uddt_nvarchar_max], 
    @i_save_mode [uddt_varchar_1], 
    @i_rec_timestamp [uddt_uid_timestamp], 
    @o_update_status [uddt_varchar_5] OUTPUT, 
    @o_service_call_ref_no [uddt_nvarchar_20] OUTPUT,
    @errorNo [errorno] OUTPUT
AS
BEGIN
    /*
     * Function to save call register
     */
    -- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
    SET NOCOUNT ON;

   
    -- The following SQL snippet illustrates (with sample values) assignment of scalar output parameters
    -- returned out of this stored procedure

    -- Use SET | SELECT for assigning values
    /*
    SET 
         @o_update_status = '' /* string */
         @o_service_call_ref_no = '' /* unicode string */
         @errorNo = ''	/* string */
     */

    /*
     * List of errors associated to this stored procedure. Use the text of the error
     * messages printed below as a guidance to set appropriate error number to @errorNo inside the procedure.
     * E_UP_005 - Update Failure : Record updated by another user. Please Retry the retrieval of the record and update.
     * E_UP_073 - Failed to save service call
     * 
     * Use the following SQL statement to set @errorNo:
     * SET @errorNo = 'One of the error numbers associated to this procedure as per list above'
     */

	declare @p_inputparam_xml xml, 
			@p_from_wf_stage_no tinyint, @p_to_wf_stage_no tinyint,
			@p_equipment_id nvarchar(30),
			@p_channel_id varchar(10),
			@p_event_date varchar(10),
			@p_event_hour varchar(2),
			@p_event_minute varchar(2),
			@p_event_date_for_autoassign  datetimeoffset(7),
			@p_customer_name nvarchar(100),
			@p_customer_address nvarchar(200),
			@p_customer_city nvarchar(60)


	declare @p_by_field_1 nvarchar(30), @p_by_field_2 nvarchar(30), @p_by_field_3 nvarchar(30), @p_by_field_4 nvarchar(30), @p_by_field_5 nvarchar(30)


	set @p_inputparam_xml = CAST(@i_call_udf_xml as XML)
	
 create table #input_params
 (paramname varchar(50) not null,
  paramval varchar(50) not null
  )
  
  insert #input_params
  (paramname, paramval)
  SELECT nodes.value('local-name(.)', 'varchar(50)'),
         nodes.value('(.)[1]', 'varchar(50)')
  FROM @p_inputparam_xml.nodes('/inputparam/*') AS Tbl(nodes)
 

	create table #applicable_custom_fields
	(
		field_type varchar(50) not null,
		applicable bit not null
	)
	
	insert #applicable_custom_fields
	(field_type, applicable)
	select field_type, applicable
	from product_customization_data_field_reference
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and information_type = 'CALL_REGISTER'
	  
set @o_update_status = '0'

declare @p_call_no int, @p_call_generated_id int,
		@p_notification_id int, @p_call_status varchar(2),
		@p_employee_id nvarchar(12)

select @p_employee_id = employee_id
from users
where company_id = @i_client_id
and country_code = @i_country_code
and user_id = @i_user_id

/* get channel from input param xml */ 
	select @p_channel_id = paramval
	from #input_params 
	where paramname = 'channel'

/* get customer name,address,city */
	
	select @p_customer_name = paramval 
	from #input_params
	where paramname = 'udf_customer_name'
	
	select @p_customer_address = paramval 
	from #input_params
	where paramname = 'udf_customer_address'
	
	select @p_customer_city = paramval 
	from #input_params
	where paramname = 'udf_customer_city'
	
if (@i_save_mode = 'A' or @i_save_mode = 'L')
begin
	if(@p_channel_id = 'mobile')
	begin
			select @i_customer_id = ltrim(rtrim(@i_customer_id)),
					@i_equipment_id = ltrim(rtrim(@i_equipment_id)),
					@i_asset_id = ltrim(rtrim(@i_asset_id))
			
				select @i_additional_description = @i_additional_description + 
					(case @i_customer_id
					 when '' then ''
					 else ' [Customer - '+@i_customer_id+'] '
					 end)+
					 (case @i_equipment_id
					  when '' then ''
					  else '[Model - '+@i_equipment_id+'] '
					  end)+
					  (case @i_asset_id
					   when '' then ''
					   else '[Asset - '+@i_asset_id + ']'
					   end)
					   
			if @i_customer_id = ''
				begin		   
					if exists (select '*' from customer 
								where company_id = @i_client_id
									and country_code = @i_country_code
									and customer_name		like '%' + @p_customer_name + '%' 
									and address_line_1		like '%' + @p_customer_address + '%' 
									and city				like '%' + @p_customer_city + '%' 
								)
					begin
						select @i_customer_id = (select top 1 customer_id from customer 
						where company_id = @i_client_id
							and country_code = @i_country_code
							and customer_name	like '%' + @p_customer_name + '%' 
							and address_line_1	like '%' + @p_customer_address + '%' 
							and city			like '%' + @p_customer_city + '%' )
					end
					else
					begin
						select  @i_customer_id				= 'ZZZ',
								@i_customer_contact_name    = 'ZZZ',
								@i_customer_location_code   = 'ZZZ'
					end	
				end
			
			if not exists (select '*' from asset_master 
							where company_id	= @i_client_id
							and   country_code	= @i_country_code 
							and   asset_id		= @i_asset_id
						  )
				begin
					 select @i_asset_id				 = 'ZZZ'
				end
			
			if not exists (select '*' from equipment 
							where company_id	= @i_client_id
							and   country_code	= @i_country_code 
							and   equipment_id	= @i_equipment_id
						  )
				begin
					 select @i_equipment_id			 = 'ZZZ'
				end   
	end
	else
	begin
		set @p_channel_id = 'WEB'
	end
	
	select @p_from_wf_stage_no = 0, @p_to_wf_stage_no = 0, @p_call_status = ''
	
	 /* Pickup the first stage and first status  for the call category */	 
	 select @p_to_wf_stage_no = workflow_stage_no 
	 from workflow_stage_master
	 where company_id = @i_client_id
	   and country_code = @i_country_code
	   and transaction_type_code = 'CALL'
	   and request_category = @i_call_category
	   and first_stage_ind = 1
	 
	 select @p_call_status = status_code 
	 from workflow_status_master
	 where company_id = @i_client_id
	   and country_code = @i_country_code
	   and transaction_type_code = 'CALL'
	   and request_category = @i_call_category
	   and first_status_ind = 1
	
	if isnull(@p_to_wf_stage_no,0) = 0 or isnull(@p_call_status,'') = ''
	begin
			set @errorNo = 'E_UP_073'
			return	
	end   
	
	/*For call loading through integration scheduler, the first status is O. 
	  This need to be corrected - Temporary fix - 5.Feb.16 - Chak */
	if @i_save_mode = 'L'
		set @p_call_status = 'O' 
	
	 insert call_register
	 (
	  company_id, country_code, customer_id,
	  customer_location_code, company_location_code,
	  call_category, call_type,
	  organogram_level_no, organogram_level_code,
	  asset_id, asset_location_code_reported,
	  equipment_id,
	  problem_description,
	  additional_information,
	  priority_code,
	  call_wf_stage_no,
	  call_status, 
	  customer_name,
	  customer_address,
	  customer_city,
	  customer_contact_name,
	  customer_contact_no,
	  customer_contact_email_id,
	  created_by_employee_id,
	  created_on_date,
	  job_order_creation_status,
	  system_user_generation_ind,
	  billable_nonbillable_ind, charges_currency_code,
	  charges_gross_amount, charges_discount_amount,
	  charges_tax_amount, charges_net_amount,
	  call_mapped_to_func_role, call_mapped_to_employee_id,
	  service_contract_doc_no,
	  udf_char_1,udf_char_2,udf_char_3,udf_char_4,
	  udf_float_1, udf_float_2, udf_float_3, udf_float_4, 
	  udf_bit_1, udf_bit_2, udf_bit_3, udf_bit_4, 
	  udf_date_1, udf_date_2, udf_date_3, udf_date_4, 
	  udf_analysis_code1, udf_analysis_code2, udf_analysis_code3, udf_analysis_code4,
	  last_update_id
	 )
	  select @i_client_id,@i_country_code, @i_customer_id ,
			 @i_customer_location_code, @i_company_location_code,
			 @i_call_category, @i_call_type,
			 @i_organogram_level_no, @i_organogram_level_code,
			 @i_asset_id, 
			 @i_asset_location_reported,
			 @i_equipment_id,
			 @i_problem_description,
			 @i_additional_description,
			 @i_priority_code,
			 @p_to_wf_stage_no, 
			 @p_call_status,
			 @p_customer_name,
			 @p_customer_address,
			 @p_customer_city,
			 @i_customer_contact_name,
			 @i_customer_contact_no,
			 @i_customer_contact_email_id,
			 @p_employee_id,
			 ( case @i_save_mode
			   when 'A' then SYSDATETIMEOFFSET()
			   when 'L' then CONVERT(datetimeoffset(7),			 
										@i_call_logged_on_date
										+' '+
										@i_call_logged_on_hour+
										':'+
										@i_call_logged_on_minute+':00'
										, 120)

			   end),       
			 'NC', /* Not Created */
			 'U',
			 @i_billable_nonbillable_ind, 
			 @i_charges_currency_code, @i_charges_gross_amount,
			 @i_charges_discount_amount, @i_charges_tax_amount,
			 @i_charges_net_amount,
		     @i_call_mapped_to_func_role, @i_call_mapped_to_employee_id,
		     @i_contract_doc_no,
			 case (select applicable from #applicable_custom_fields
							where field_type = 'udf_char_1')
			 when 1 then 	isnull( (select paramval from #input_params where paramname = 'udf_char_1') ,'')	
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
			  	   where field_type = 'udf_char_2')
			 when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_2') ,'')
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_char_3')
			 when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_3') ,'')
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_char_4')
			 when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_4') ,'')
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_float_1')
			 when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_1') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_float_2')
			 when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_2') ,'')
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_float_3')
			 when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_3') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_float_4')
			 when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_4') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_bit_1')
			 when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_1') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_bit_2')
			 when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_2') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_bit_3')
			 when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_3') ,0)
				else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_bit_4')
			 when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_4') ,0)
			 else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_date_1')
			 when 1 then 
				isnull( (select CONVERT(datetimeoffset,
				 (select x.paramval from #input_params x where x.paramname = 'udf_date_1')
				+' ' +
				 (select y.paramval from #input_params y where y.paramname = 'udf_date_1_hour')
				 + ':' + 
				 (select z.paramval from #input_params z where z.paramname = 'udf_date_1_minute')
				 +':00',120)), sysdatetimeoffset())
				else NULL
			 end,
			 case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_date_2')
				when 1 then 
				isnull( (select CONVERT(datetimeoffset,
				 (select x.paramval from #input_params x where x.paramname = 'udf_date_2')
				+' ' +
				 (select y.paramval from #input_params y where y.paramname = 'udf_date_2_hour')
				 + ':' + 
				 (select z.paramval from #input_params z where z.paramname = 'udf_date_2_minute')
				 +':00',120)), sysdatetimeoffset())
				else NULL
			   end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_date_3')
				when 1 then
				isnull( (select CONVERT(datetimeoffset,
				 (select x.paramval from #input_params x where x.paramname = 'udf_date_3')
				+' ' +
				 (select y.paramval from #input_params y where y.paramname = 'udf_date_3_hour')
				 + ':' + 
				 (select z.paramval from #input_params z where z.paramname = 'udf_date_3_minute')
				 +':00',120)), sysdatetimeoffset())
				else NULL
			   end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_date_4')
			  when 1 then 
				isnull( (select CONVERT(datetimeoffset,
				 (select x.paramval from #input_params x where x.paramname = 'udf_date_4')
				+' ' +
				 (select y.paramval from #input_params y where y.paramname = 'udf_date_4_hour')
				 + ':' + 
				 (select z.paramval from #input_params z where z.paramname = 'udf_date_4_minute')
				 +':00',120)), sysdatetimeoffset())
			  else NULL
			  end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_analysis_code1')
			  when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code1') ,'')
			  else NULL
			  end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_analysis_code2')
			  when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code2') ,'')
			  else NULL
			  end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_analysis_code3')
			  when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code3') ,'')
			  else NULL
			  end,
			  case (select applicable from #applicable_custom_fields
					 where field_type = 'udf_analysis_code4')
			  when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code4') ,'')
			  else NULL
			  end,
			  @i_user_id
	         
	select @p_call_generated_id = @@IDENTITY
	
  if ( @@ROWCOUNT != 0) 
  begin

	if @i_save_mode = 'A'
	begin
	    
		/* Generate sequence no */
		execute sp_get_client_specific_callnogen_fields @i_client_id, @i_country_code, @i_call_category, @i_call_type, @p_by_field_1 OUTPUT, @p_by_field_2 OUTPUT, @p_by_field_3 OUTPUT, @p_by_field_4 OUTPUT, @p_by_field_5 OUTPUT
	    
		execute sp_create_new_sequence_no @i_session_id, @i_client_id, @i_user_id, @i_locale_id, @i_country_code,'CALL',@p_by_field_1, @p_by_field_2, @p_by_field_3,@p_by_field_4, @p_by_field_5, @p_call_no OUTPUT
	    
		if @p_call_no = '0'
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
		update call_register
		set call_ref_no = dbo.fn_client_specific_stamping_call_no(@i_client_id, @i_country_code, @i_call_category, @i_call_type,@p_call_no)
		where company_id = @i_client_id
		  and country_code = @i_country_code
		  and call_id = @p_call_generated_id
  
    end
    else if @i_save_mode = 'L' /* Called from Loading program with service call no already provided by source system*/
    begin
    
		update call_register
		set call_ref_no = @o_service_call_ref_no
		where company_id = @i_client_id
		  and country_code = @i_country_code
		  and call_id = @p_call_generated_id
    
    end
    
    if (@@ROWCOUNT != 0)
    begin
       
       select @o_service_call_ref_no = call_ref_no,
              @o_update_status = 'SP001'
       from call_register
       where company_id = @i_client_id
         and country_code = @i_country_code
         and call_id = @p_call_generated_id
         

	  insert call_status_event_log
	  (
		company_id, country_code, call_ref_no,channel_id,
		eventverb_id,
		from_wf_stage_no, to_wf_stage_no, event_date, to_status, from_status,
		by_employee_id, last_update_id
	  )
	  select @i_client_id, @i_country_code, @o_service_call_ref_no, @p_channel_id,
			'OPEN',
			@p_from_wf_stage_no, @p_to_wf_stage_no, SYSDATETIMEOFFSET(), 'O','', @p_employee_id, @i_user_id
			
	  
	  if @@ROWCOUNT = 0
	  begin
			set @errorNo = 'E_UP_073'
			return
	  end        
       
    /* Include call register dependencies like resource list, part list*/
    if @i_asset_id != 'ZZZ'
    begin
    
		/* If there is a contract for customer and a visit, create a link*/
		if @i_contract_doc_no != ''
		begin
			
			update asset_service_schedule
			set service_visit_status = 'SP',
				call_jo_ind = 'C',
				call_ref_jo_no = @o_service_call_ref_no,
				last_update_id = @i_user_id
			where company_id = @i_client_id
			  and country_code = @i_country_code
			  and asset_id = @i_asset_id
			  and contract_doc_no = @i_contract_doc_no
			  and service_visit_slno = @i_contract_visit_no
	
			if @@ROWCOUNT = 0
			begin
				set @errorNo = 'E_UP_073'
				return
			end        
      		  
		end
		
		select @p_equipment_id = equipment_id
		from asset_master
		where company_id = @i_client_id
		  and country_code = @i_country_code
		  and asset_id = @i_asset_id
		  
		execute sp_save_manage_call_register_dependencies @i_user_id, 
			@i_client_id, @i_country_code,@o_service_call_ref_no,
			@p_equipment_id , @o_update_status OUTPUT

		if @o_update_status = 'ER001'
		begin
			set @errorNo = 'E_UP_073'
			return		
		end	
    end
       
	/* Determine if there are any events configured in notification rules */
	
	declare @p_request_category varchar(10),
			@p_request_type nvarchar(10),
			@p_organogram_level_no tinyint,
			@p_organogram_level_code nvarchar(15),
			@p_company_location_code nvarchar(8),
			@p_notification_event_code_1 nvarchar(60),
			@p_notification_event_code_2 nvarchar(60),
			@p_notification_event_code_3 nvarchar(60),
			@p_notification_event_code_4 nvarchar(60),
			@p_notification_event_code_5 nvarchar(60),
			@p_notification_xml nvarchar(max)
		
				
	select @p_request_category = call_category,
			@p_request_type = call_type,
			@p_organogram_level_no = organogram_level_no,
			@p_organogram_level_code = organogram_level_code,
			@p_company_location_code = company_location_code		
	from call_register
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and call_ref_no = @o_service_call_ref_no
	  
	select @p_notification_event_code_1 = isnull(@p_notification_event_code_1,isnull(notification_event_code_1,'')),
			   @p_notification_event_code_2 = isnull(@p_notification_event_code_2,isnull(notification_event_code_2,'')),
			   @p_notification_event_code_3 = isnull(@p_notification_event_code_3,isnull(notification_event_code_3,'')),
			   @p_notification_event_code_4 = isnull(@p_notification_event_code_4,isnull(notification_event_code_4,'')),
			   @p_notification_event_code_5 = isnull(@p_notification_event_code_5, isnull(notification_event_code_5,''))	
	from company_notification_rules
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and transaction_type_code = 'CALL'
	  and transaction_subtype_code = 'CALL'
	  and request_category = @p_request_category 
	  and request_type = 'ALL'
	  and company_location_code = 'ALL'
	  and organogram_level_no =  'ALL'
	  and organogram_level_code= 'ALL'
	  and attachment_type = 'W'
	  and wf_infromto_ind = 'FT'
	  and from_wf_stage = 'NA'
	  and from_wf_status = 'NA'
	  and to_wf_stage = '1'
	  and to_wf_status = 'O'
	
	select @p_notification_event_code_1 = isnull(@p_notification_event_code_1,isnull(notification_event_code_1,'')),
		   @p_notification_event_code_2 = isnull(@p_notification_event_code_2,isnull(notification_event_code_2,'')),
		   @p_notification_event_code_3 = isnull(@p_notification_event_code_3,isnull(notification_event_code_3,'')),
		   @p_notification_event_code_4 = isnull(@p_notification_event_code_4,isnull(notification_event_code_4,'')),
		   @p_notification_event_code_5 = isnull(@p_notification_event_code_5, isnull(notification_event_code_5,''))	
	from company_notification_rules
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and transaction_type_code = 'CALL'
	  and transaction_subtype_code = 'CALL'
	  and request_category = @p_request_category 
	  and request_type = @p_request_type
	  and company_location_code = 'ALL'
	  and organogram_level_no =  'ALL'
	  and organogram_level_code= 'ALL'
	  and attachment_type = 'W'
	  and wf_infromto_ind = 'FT'
	  and from_wf_stage = 'NA'
	  and from_wf_status = 'NA'
	  and to_wf_stage = '1'
	  and to_wf_status = 'O'

	select @p_notification_event_code_1 = isnull(@p_notification_event_code_1,isnull(notification_event_code_1,'')),
		   @p_notification_event_code_2 = isnull(@p_notification_event_code_2,isnull(notification_event_code_2,'')),
		   @p_notification_event_code_3 = isnull(@p_notification_event_code_3,isnull(notification_event_code_3,'')),
		   @p_notification_event_code_4 = isnull(@p_notification_event_code_4,isnull(notification_event_code_4,'')),
		   @p_notification_event_code_5 = isnull(@p_notification_event_code_5, isnull(notification_event_code_5,''))	
	from company_notification_rules
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and transaction_type_code = 'CALL'
	  and transaction_subtype_code = 'CALL'
	  and request_category = @p_request_category 
	  and request_type = @p_request_type
	  and company_location_code = @p_company_location_code
	  and organogram_level_no =  'ALL'
	  and organogram_level_code= 'ALL'
	  and attachment_type = 'W'
	  and wf_infromto_ind = 'FT'
	  and from_wf_stage = 'NA'
	  and from_wf_status = 'NA'
	  and to_wf_stage = '1'
	  and to_wf_status = 'O'

	select @p_notification_event_code_1 = isnull(@p_notification_event_code_1,isnull(notification_event_code_1,'')),
			   @p_notification_event_code_2 = isnull(@p_notification_event_code_2,isnull(notification_event_code_2,'')),
			   @p_notification_event_code_3 = isnull(@p_notification_event_code_3,isnull(notification_event_code_3,'')),
			   @p_notification_event_code_4 = isnull(@p_notification_event_code_4,isnull(notification_event_code_4,'')),
			   @p_notification_event_code_5 = isnull(@p_notification_event_code_5, isnull(notification_event_code_5,''))	
	from company_notification_rules
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and transaction_type_code = 'CALL'
	  and transaction_subtype_code = 'CALL'
	  and request_category = @p_request_category 
	  and request_type = @p_request_type
	  and company_location_code = @p_company_location_code
	  and organogram_level_no =  @p_organogram_level_no
	  and organogram_level_code= 'ALL'
	  and attachment_type = 'W'
	  and wf_infromto_ind = 'FT'
	  and from_wf_stage = 'NA'
	  and from_wf_status = 'NA'
	  and to_wf_stage = '1'
	  and to_wf_status = 'O'
	
	select @p_notification_event_code_1 = isnull(@p_notification_event_code_1,isnull(notification_event_code_1,'')),
			   @p_notification_event_code_2 = isnull(@p_notification_event_code_2,isnull(notification_event_code_2,'')),
			   @p_notification_event_code_3 = isnull(@p_notification_event_code_3,isnull(notification_event_code_3,'')),
			   @p_notification_event_code_4 = isnull(@p_notification_event_code_4,isnull(notification_event_code_4,'')),
			   @p_notification_event_code_5 = isnull(@p_notification_event_code_5, isnull(notification_event_code_5,''))	
	from company_notification_rules
	where company_id = @i_client_id
	  and country_code = @i_country_code
	  and transaction_type_code = 'CALL'
	  and transaction_subtype_code = 'CALL'
	  and request_category = @p_request_category 
	  and request_type = @p_request_type
	  and company_location_code = @p_company_location_code
	  and organogram_level_no =  @p_organogram_level_no
	  and organogram_level_code= @p_organogram_level_code
	  and attachment_type = 'W'
	  and wf_infromto_ind = 'FT'
	  and from_wf_stage = 'NA'
	  and from_wf_status = 'NA'
	  and to_wf_stage = '1'
	  and to_wf_status = 'O'

	if isnull(@p_notification_event_code_1,'') != ''
	begin
	
	if exists ( select 1 from company_notification a
				where a.company_id = @i_client_id
				  and a.country_code = @i_country_code
				  and a.notification_event_code = @p_notification_event_code_1
				  and active_ind = 1)
	begin
		
		 select @p_notification_xml = '<notification_info>'+
							'<call_no>'+@o_service_call_ref_no+'</call_no>'+
							'<call_type>'+
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
							end 
							+'</call_type>'+
							'<cust_id>'+isnull(c.customer_id,'')+'</cust_id>'+
							'<cust_name>'+isnull(substring(c.customer_name,1,50),'') +'</cust_name>'+
							'<cust_contact_name>'+isnull(substring(a.customer_contact_name,1,50),'') +'</cust_contact_name>'+
							'<cust_contact_no>'+isnull(a.customer_contact_no,'') +'</cust_contact_no>'+
							'<cust_contact_email_id>'+ISNULL(a.customer_contact_email_id,'')+'</cust_contact_email_id>'+
							'<description>'+isnull(a.problem_description,'')+'</description>'+
							'<call_logged_on_date>'+CONVERT(varchar(17),a.created_on_date,113)+'</call_logged_on_date>'+
							'<udf_char_1>'+isnull(a.udf_char_1,'')+'</udf_char_1>'+												 
							'<udf_date_1>'+CONVERT(varchar(20),a.udf_date_1,100)+'</udf_date_1>'+												 
							'<support_desk_no>Service Coordinator</support_desk_no>'+
							'</notification_info>'		
		  from call_register a
		  left outer join customer c
		  on a.company_id = c.company_id
			and a.country_code = c.country_code
			and a.customer_id = c.customer_id
		  where a.company_id = @i_client_id
			and a.country_code = @i_country_code
			and a.call_ref_no = @o_service_call_ref_no
	
		execute sp_log_new_notification  @i_session_id, @i_user_id  , @i_client_id , 
		  @i_locale_id , @i_country_code , @p_notification_event_code_1 ,@p_notification_xml, @i_user_id, @p_notification_id OUTPUT
		
		if @p_notification_id = 0
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
	end
	end
	
	if isnull(@p_notification_event_code_2,'') != ''
	begin
	
	if exists ( select 1 from company_notification a
				where a.company_id = @i_client_id
				  and a.country_code = @i_country_code
				  and a.notification_event_code = @p_notification_event_code_2
				  and active_ind = 1)
	begin
		
		 select @p_notification_xml = '<notification_info>'+
							'<call_no>'+@o_service_call_ref_no+'</call_no>'+
							'<call_type>'+
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
							end 
							+'</call_type>'+
							'<cust_id>'+isnull(c.customer_id,'')+'</cust_id>'+
							'<cust_name>'+isnull(substring(c.customer_name,1,50),'') +'</cust_name>'+
							'<cust_contact_name>'+isnull(substring(a.customer_contact_name,1,50),'') +'</cust_contact_name>'+
							'<cust_contact_no>'+isnull(a.customer_contact_no,'') +'</cust_contact_no>'+
							'<cust_contact_email_id>'+ISNULL(a.customer_contact_email_id,'')+'</cust_contact_email_id>'+
							'<description>'+isnull(a.problem_description,'')+'</description>'+
							'<call_logged_on_date>'+CONVERT(varchar(17),a.created_on_date,113)+'</call_logged_on_date>'+
							'<udf_char_1>'+isnull(a.udf_char_1,'')+'</udf_char_1>'+												 
							'<udf_date_1>'+CONVERT(varchar(20),a.udf_date_1,100)+'</udf_date_1>'+												 
							'<support_desk_no>Service Coordinator</support_desk_no>'+
							'</notification_info>'	
		  from call_register a
		  left outer join customer c
		  on a.company_id = c.company_id
			and a.country_code = c.country_code
			and a.customer_id = c.customer_id
		  where a.company_id = @i_client_id
			and a.country_code = @i_country_code
			and a.call_ref_no = @o_service_call_ref_no
	
		execute sp_log_new_notification  @i_session_id, @i_user_id  , @i_client_id , 
		  @i_locale_id , @i_country_code , @p_notification_event_code_2 ,@p_notification_xml, @i_user_id, @p_notification_id OUTPUT
		
		if @p_notification_id = 0
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
	end
	end

	if isnull(@p_notification_event_code_3,'') != ''
	begin
	
	if exists ( select 1 from company_notification a
				where a.company_id = @i_client_id
				  and a.country_code = @i_country_code
				  and a.notification_event_code = @p_notification_event_code_3
				  and active_ind = 1)
	begin
		
		 select @p_notification_xml = '<notification_info>'+
							'<call_no>'+cast(@o_service_call_ref_no as varchar(5))+'</call_no>'+
	'<call_type>'+
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
							end 
							+'</call_type>'+
							'<cust_id>'+isnull(c.customer_id,'')+'</cust_id>'+
							'<cust_name>'+isnull(substring(c.customer_name,1,50),'') +'</cust_name>'+
							'<cust_contact_name>'+isnull(substring(a.customer_contact_name,1,50),'') +'</cust_contact_name>'+
							'<cust_contact_no>'+isnull(a.customer_contact_no,'') +'</cust_contact_no>'+
							'<cust_contact_email_id>'+ISNULL(a.customer_contact_email_id,'')+'</cust_contact_email_id>'+
							'<description>'+isnull(a.problem_description,'')+'</description>'+
							'<call_logged_on_date>'+CONVERT(varchar(17), a.created_on_date,113)+'</call_logged_on_date>'+
							'<udf_char_1>'+isnull(a.udf_char_1,'')+'</udf_char_1>'+												 
							'<udf_char_2>'+isnull(a.udf_char_2,'')+'</udf_char_2>'+												 
							'<udf_char_3>'+isnull(a.udf_char_3,'')+'</udf_char_3>'+												 
							'<udf_char_4>'+isnull(a.udf_char_4,'')+'</udf_char_4>'+												 
							'<udf_bit_1>'+isnull(cast(a.udf_bit_1 as varchar(1)),'')+'</udf_bit_1>'+												 
							'<udf_bit_2>'+isnull(cast(a.udf_bit_2 as varchar(1)),'')+'</udf_bit_2>'+												 
							'<udf_bit_3>'+isnull(cast(a.udf_bit_3 as varchar(1)),'')+'</udf_bit_3>'+												 
							'<udf_bit_4>'+isnull(cast(a.udf_bit_4 as varchar(1)),'')+'</udf_bit_4>'+												 
							'<udf_float_1>'+isnull(cast(a.udf_float_1 as varchar(14)),'')+'</udf_float_1>'+												 
							'<udf_float_2>'+isnull(cast(a.udf_float_2 as varchar(14)),'')+'</udf_float_2>'+												 
							'<udf_float_3>'+isnull(cast(a.udf_float_3 as varchar(14)),'')+'</udf_float_3>'+												 
							'<udf_float_4>'+isnull(cast(a.udf_float_4 as varchar(14)),'')+'</udf_float_4>'+												 
							'<udf_date_1>'+isnull(CONVERT(varchar(20),a.udf_date_1,100),'')+'</udf_date_1>'+												 
							'<udf_date_2>'+isnull(CONVERT(varchar(20),a.udf_date_2,100),'')+'</udf_date_2>'+												 
							'<udf_date_3>'+isnull(CONVERT(varchar(20),a.udf_date_3,100),'')+'</udf_date_3>'+												 
							'<udf_date_4>'+isnull(CONVERT(varchar(20),a.udf_date_4,100),'')+'</udf_date_4>'+												 
							'<udf_analysis_code1>'+isnull(a.udf_analysis_code1,'')+'</udf_analysis_code1>'+												 
							'<udf_analysis_code2>'+isnull(a.udf_analysis_code2,'')+'</udf_analysis_code2>'+												 
							'<udf_analysis_code3>'+isnull(a.udf_analysis_code3,'')+'</udf_analysis_code3>'+												 
							'<udf_analysis_code4>'+isnull(a.udf_analysis_code4,'')+'</udf_analysis_code4>'+												 
							'<support_desk_no>Service Coordinator</support_desk_no>'+
							'</notification_info>'	
		  from call_register a
		  left outer join customer c
		  on a.company_id = c.company_id
			and a.country_code = c.country_code
			and a.customer_id = c.customer_id
		  where a.company_id = @i_client_id
			and a.country_code = @i_country_code
			and a.call_ref_no = @o_service_call_ref_no
	
		execute sp_log_new_notification  @i_session_id, @i_user_id  , @i_client_id , 
		  @i_locale_id , @i_country_code , @p_notification_event_code_3 ,@p_notification_xml, @i_user_id, @p_notification_id OUTPUT
		
		if @p_notification_id = 0
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
	end
	end


	if isnull(@p_notification_event_code_4,'') != ''
	begin
	
	if exists ( select 1 from company_notification a
				where a.company_id = @i_client_id
				  and a.country_code = @i_country_code
				  and a.notification_event_code = @p_notification_event_code_4
				  and active_ind = 1
				  )
	begin
		
		 select @p_notification_xml = '<notification_info>'+
							'<call_no>'+cast(@o_service_call_ref_no as varchar(5))+'</call_no>'+
							'<call_type>'+
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
							end 
							+'</call_type>'+
							'<cust_id>'+isnull(c.customer_id,'')+'</cust_id>'+
							'<cust_name>'+isnull(substring(c.customer_name,1,50),'') +'</cust_name>'+
							'<cust_contact_name>'+isnull(substring(a.customer_contact_name,1,50),'') +'</cust_contact_name>'+
							'<cust_contact_no>'+isnull(a.customer_contact_no,'') +'</cust_contact_no>'+
							'<cust_contact_email_id>'+ISNULL(a.customer_contact_email_id,'')+'</cust_contact_email_id>'+
							'<description>'+isnull(a.problem_description,'')+'</description>'+
							'<call_logged_on_date>'+CONVERT(varchar(17), a.created_on_date,113)+'</call_logged_on_date>'+
							'<udf_char_1>'+isnull(a.udf_char_1,'')+'</udf_char_1>'+												 
							'<udf_char_2>'+isnull(a.udf_char_2,'')+'</udf_char_2>'+												 
							'<udf_char_3>'+isnull(a.udf_char_3,'')+'</udf_char_3>'+												 
							'<udf_char_4>'+isnull(a.udf_char_4,'')+'</udf_char_4>'+												 
							'<udf_bit_1>'+isnull(cast(a.udf_bit_1 as varchar(1)),'')+'</udf_bit_1>'+												 
							'<udf_bit_2>'+isnull(cast(a.udf_bit_2 as varchar(1)),'')+'</udf_bit_2>'+												 
							'<udf_bit_3>'+isnull(cast(a.udf_bit_3 as varchar(1)),'')+'</udf_bit_3>'+												 
							'<udf_bit_4>'+isnull(cast(a.udf_bit_4 as varchar(1)),'')+'</udf_bit_4>'+												 
							'<udf_float_1>'+isnull(cast(a.udf_float_1 as varchar(14)),'')+'</udf_float_1>'+												 
							'<udf_float_2>'+isnull(cast(a.udf_float_2 as varchar(14)),'')+'</udf_float_2>'+												 
							'<udf_float_3>'+isnull(cast(a.udf_float_3 as varchar(14)),'')+'</udf_float_3>'+												 
							'<udf_float_4>'+isnull(cast(a.udf_float_4 as varchar(14)),'')+'</udf_float_4>'+												 
							'<udf_date_1>'+isnull(CONVERT(varchar(20),a.udf_date_1,100),'')+'</udf_date_1>'+												 
							'<udf_date_2>'+isnull(CONVERT(varchar(20),a.udf_date_2,100),'')+'</udf_date_2>'+												 
							'<udf_date_3>'+isnull(CONVERT(varchar(20),a.udf_date_3,100),'')+'</udf_date_3>'+												 
							'<udf_date_4>'+isnull(CONVERT(varchar(20),a.udf_date_4,100),'')+'</udf_date_4>'+												 
							'<udf_analysis_code1>'+isnull(a.udf_analysis_code1,'')+'</udf_analysis_code1>'+												 
							'<udf_analysis_code2>'+isnull(a.udf_analysis_code2,'')+'</udf_analysis_code2>'+												 
							'<udf_analysis_code3>'+isnull(a.udf_analysis_code3,'')+'</udf_analysis_code3>'+												 
							'<udf_analysis_code4>'+isnull(a.udf_analysis_code4,'')+'</udf_analysis_code4>'+												 
							'<support_desk_no>Service Coordinator</support_desk_no>'+
							'</notification_info>'	
		  from call_register a
		  left outer join customer c
		  on a.company_id = c.company_id
			and a.country_code = c.country_code
			and a.customer_id = c.customer_id
		  where a.company_id = @i_client_id
			and a.country_code = @i_country_code
			and a.call_ref_no = @o_service_call_ref_no
	
		execute sp_log_new_notification  @i_session_id, @i_user_id  , @i_client_id , 
		  @i_locale_id , @i_country_code , @p_notification_event_code_4 ,@p_notification_xml, @i_user_id, @p_notification_id OUTPUT
		
		if @p_notification_id = 0
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
	end
	end


	if isnull(@p_notification_event_code_5,'') != ''
	begin
	
	if exists ( select 1 from company_notification a
				where a.company_id = @i_client_id
				  and a.country_code = @i_country_code
				  and a.notification_event_code = @p_notification_event_code_5
				  and active_ind = 1
				  )
	begin
		
		 select @p_notification_xml = '<notification_info>'+
							'<call_no>'+cast(@o_service_call_ref_no as varchar(5))+'</call_no>'+
	'<call_type>'+
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
							end 
							+'</call_type>'+
							'<cust_id>'+isnull(c.customer_id,'')+'</cust_id>'+
							'<cust_name>'+isnull(substring(c.customer_name,1,50),'') +'</cust_name>'+
							'<cust_contact_name>'+isnull(substring(a.customer_contact_name,1,50),'') +'</cust_contact_name>'+
							'<cust_contact_no>'+isnull(a.customer_contact_no,'') +'</cust_contact_no>'+
							'<cust_contact_email_id>'+ISNULL(a.customer_contact_email_id,'')+'</cust_contact_email_id>'+
							'<description>'+isnull(a.problem_description,'')+'</description>'+
							'<call_logged_on_date>'+CONVERT(varchar(17), a.created_on_date, 113)+'</call_logged_on_date>'+
							'<udf_char_1>'+isnull(a.udf_char_1,'')+'</udf_char_1>'+												 
							'<udf_char_2>'+isnull(a.udf_char_2,'')+'</udf_char_2>'+												 
							'<udf_char_3>'+isnull(a.udf_char_3,'')+'</udf_char_3>'+												 
							'<udf_char_4>'+isnull(a.udf_char_4,'')+'</udf_char_4>'+												 
							'<udf_bit_1>'+isnull(cast(a.udf_bit_1 as varchar(1)),'')+'</udf_bit_1>'+												 
							'<udf_bit_2>'+isnull(cast(a.udf_bit_2 as varchar(1)),'')+'</udf_bit_2>'+												 
							'<udf_bit_3>'+isnull(cast(a.udf_bit_3 as varchar(1)),'')+'</udf_bit_3>'+												 
							'<udf_bit_4>'+isnull(cast(a.udf_bit_4 as varchar(1)),'')+'</udf_bit_4>'+												 
							'<udf_float_1>'+isnull(cast(a.udf_float_1 as varchar(14)),'')+'</udf_float_1>'+												 
							'<udf_float_2>'+isnull(cast(a.udf_float_2 as varchar(14)),'')+'</udf_float_2>'+												 
							'<udf_float_3>'+isnull(cast(a.udf_float_3 as varchar(14)),'')+'</udf_float_3>'+												 
							'<udf_float_4>'+isnull(cast(a.udf_float_4 as varchar(14)),'')+'</udf_float_4>'+												 
							'<udf_date_1>'+isnull(CONVERT(varchar(20),a.udf_date_1,100),'')+'</udf_date_1>'+												 
							'<udf_date_2>'+isnull(CONVERT(varchar(20),a.udf_date_2,100),'')+'</udf_date_2>'+												 
							'<udf_date_3>'+isnull(CONVERT(varchar(20),a.udf_date_3,100),'')+'</udf_date_3>'+												 
							'<udf_date_4>'+isnull(CONVERT(varchar(20),a.udf_date_4,100),'')+'</udf_date_4>'+												 
							'<udf_analysis_code1>'+isnull(a.udf_analysis_code1,'')+'</udf_analysis_code1>'+												 
							'<udf_analysis_code2>'+isnull(a.udf_analysis_code2,'')+'</udf_analysis_code2>'+												 
							'<udf_analysis_code3>'+isnull(a.udf_analysis_code3,'')+'</udf_analysis_code3>'+												 
							'<udf_analysis_code4>'+isnull(a.udf_analysis_code4,'')+'</udf_analysis_code4>'+												 
							'<support_desk_no>Service Coordinator</support_desk_no>'+
							'</notification_info>'	
		  from call_register a
		  left outer join customer c
		  on a.company_id = c.company_id
			and a.country_code = c.country_code
			and a.customer_id = c.customer_id
		  where a.company_id = @i_client_id
			and a.country_code = @i_country_code
			and a.call_ref_no = @o_service_call_ref_no
	
		execute sp_log_new_notification  @i_session_id, @i_user_id  , @i_client_id , 
		  @i_locale_id , @i_country_code , @p_notification_event_code_5 ,@p_notification_xml, @i_user_id, @p_notification_id OUTPUT
		
		if @p_notification_id = 0
		begin
			set @errorNo = 'E_UP_073'
			return
		end
		
	end
	end

    		        
	end
    else 
    begin
		set @errorNo = 'E_UP_073'
        select @o_service_call_ref_no = ''
    end
  end
  /* Run Auto Assignment for declaring variable */
	declare @p_assigned_on_date varchar(10), @p_assigned_on_hour varchar(2),
			@p_assigned_on_minute varchar(2), @p_sch_finish_date varchar(10), @p_sch_finish_hour varchar(2),
			@p_sch_finish_minute varchar(2), @p_assigned_to_emp_id nvarchar(12)
	declare @p_current_datetime varchar(20), @p_finish_datetime varchar(20)
  
	if @i_client_id in ( 'titan','ab','nac')
	begin
	
		/* Only for Titan , NAC, AB*/

	  select @p_current_datetime = CONVERT(varchar(20), sysdatetimeoffset(),121),
			 @p_finish_datetime = convert(varchar(20),DATEADD(hh, 24, SYSDATETIMEOFFSET()),121)
	    
	  select @p_assigned_on_date = SUBSTRING(@p_current_datetime,1,10), 
			 @p_assigned_on_hour = SUBSTRING(@p_current_datetime, 12,2),
			 @p_assigned_on_minute = SUBSTRING(@p_current_datetime,15,2)
	 
	  select @p_sch_finish_date = SUBSTRING(@p_finish_datetime,1,10), 
			 @p_sch_finish_hour = SUBSTRING(@p_finish_datetime, 12,2),
			 @p_sch_finish_minute = SUBSTRING(@p_finish_datetime,15,2)
	  
	  select @p_assigned_to_emp_id = ''
	  
	  select @p_assigned_to_emp_id = employee_id
	  from customer_mapping_to_employee
	  where company_id = @i_client_id
		and country_code = @i_country_code
		and mapping_purpose_code = 'CALLAUTOASSIGNMENT'
		and request_category = @i_call_category
		and request_type = @i_call_type
		and customer_id = @i_customer_id
		and customer_location_code = 'ALL' /* Pending: Need to go through order of matching*/

	  if @p_assigned_to_emp_id = ''
		  select @p_assigned_to_emp_id = employee_id
		  from customer_mapping_to_employee
		  where company_id = @i_client_id
			and country_code = @i_country_code
			and mapping_purpose_code = 'CALLAUTOASSIGNMENT'
			and request_category = 'ALL'
			and request_type = 'ALL'
			and customer_id = @i_customer_id
			and customer_location_code = 'ALL'
			
	if (@p_channel_id = 'mobile' and  @p_assigned_to_emp_id != '')
			select @p_assigned_to_emp_id = paramval
			from #input_params 
			where paramname = 'assigned_to_emp_id'
		        
	  execute sp_save_manage_call_assignment
	  @i_client_id, @i_country_code, @i_session_id, @i_user_id, @i_locale_id, 
	  @o_service_call_ref_no,
	  @p_assigned_to_emp_id,
	  @p_assigned_on_date, @p_assigned_on_hour, @p_assigned_on_minute,
	  @p_assigned_on_date, @p_assigned_on_hour, @p_assigned_on_minute,
	  @p_sch_finish_date, @p_sch_finish_hour, @p_sch_finish_minute,
	  24,'h',24,'h',@o_update_status OUTPUT, @errorNo OUTPUT
	  
	  if @errorNo != ''
	  begin
		set @errorNo = 'E_UP_073'
		return  
	  end
	  
	  /* Run start of the call */
	  
	  execute sp_update_call_start
	  @i_session_id, @i_user_id, @i_client_id, @i_locale_id, @i_country_code, 
	  @o_service_call_ref_no, 
	  @p_assigned_on_date, @p_assigned_on_hour, @p_assigned_on_minute,
	  @p_assigned_on_date, @p_assigned_on_hour, @p_assigned_on_minute,
	  @i_rec_timestamp, @o_update_status OUTPUT, @errorNo OUTPUT
	  
	  
	  if @errorNo != ''
	  begin
		set @errorNo = 'E_UP_073'
		return  
	  end  
	end /* if client_id = 'titan' */
	
		if @p_channel_id = 'mobile'
		begin
			  /* Run Auto Assignment */
			  select @p_assigned_to_emp_id = ''
			 
			  select @p_current_datetime = CONVERT(varchar(20), sysdatetimeoffset(),121),
					 @p_finish_datetime = convert(varchar(20),DATEADD(hh, 24, SYSDATETIMEOFFSET()),121)
			    
			  select @p_assigned_on_date = SUBSTRING(@p_current_datetime,1,10), 
					 @p_assigned_on_hour = SUBSTRING(@p_current_datetime, 12,2),
					 @p_assigned_on_minute = SUBSTRING(@p_current_datetime,15,2)
			 
			  select @p_sch_finish_date = SUBSTRING(@p_finish_datetime,1,10), 
					 @p_sch_finish_hour = SUBSTRING(@p_finish_datetime, 12,2),
					 @p_sch_finish_minute = SUBSTRING(@p_finish_datetime,15,2)

			  select @p_assigned_to_emp_id = paramval
			  from #input_params 
			  where paramname = 'assigned_to_emp_id'
			
			/* If Assigned to Self, Pick employee id from users */
			
				
			if (@p_assigned_to_emp_id != '' )
			begin
				
				 declare @p_assigned_to_user_id nvarchar(12),
						@p_from_call_status varchar(2),
						@p_by_employee_id nvarchar(12),
						@p_update_status varchar(5), @p_error_code varchar(10),
						@p_current_wf_stage_no tinyint, @p_current_status varchar(2),
						@p_to_call_status varchar(2), @p_eventverb_id varchar(60),
						@p_inputparam_xml1 nvarchar(max),
						@p_record_timestamp varchar(36)
	
				set @p_eventverb_id = 'ASSIGN'
					
				select @p_current_wf_stage_no = call_wf_stage_no,
					   @p_current_status = call_status,
					   @p_request_category = call_category,
					   @p_request_type = call_type
				from call_register
				where company_id = @i_client_id
				  and country_code = @i_country_code
				  and call_ref_no = @o_service_call_ref_no
				  
					select  @p_from_wf_stage_no = from_workflow_stage,
							@p_from_call_status =  from_status,
							@p_to_wf_stage_no = to_workflow_stage,
							@p_to_call_status =   to_status 
					from workflow_eventverb_list
					where company_id = @i_client_id
					  and country_code = @i_country_code
					  and transaction_type_code  = 'CALL'
					  and request_category = @p_request_category
					  and request_type in ('ALL', @p_request_type)
					  and eventverb_id = @p_eventverb_id
					  and from_workflow_stage = @p_current_wf_stage_no
					  and from_status = @p_current_status
				
				select @p_by_employee_id = employee_id
				from users
				where company_id = @i_client_id
				  and country_code = @i_country_code
				  and user_id = @i_user_id	
				
				select @p_request_category = call_category,
						@p_request_type = call_type,
						@p_organogram_level_no = organogram_level_no,
						@p_organogram_level_code = organogram_level_code,
						@p_company_location_code = company_location_code,
						@p_record_timestamp = cast(convert(uniqueidentifier,cast(last_update_timestamp as binary)) as varchar(36))	
				from call_register
				where company_id = @i_client_id
				  and country_code = @i_country_code
				  and call_ref_no = @o_service_call_ref_no
				  
				select @p_inputparam_xml1 = '<inputparam>
											 <call_register_sch_start_date>'+@p_assigned_on_date+'</call_register_sch_start_date>
											 <call_register_sch_start_date_hour>'+@p_assigned_on_hour+'</call_register_sch_start_date_hour>
											 <call_register_sch_start_date_minute>'+@p_assigned_on_minute+'</call_register_sch_start_date_minute>	
											 <call_register_sch_finish_date>'+@p_sch_finish_date+'</call_register_sch_finish_date>
											 <call_register_sch_finish_date_hour>'+@p_sch_finish_hour+'</call_register_sch_finish_date_hour>
											 <call_register_sch_finish_date_minute>'+@p_sch_finish_minute+'</call_register_sch_finish_date_minute>	
											 </inputparam>'
				
				select @p_event_date_for_autoassign = DATEADD(minute, 5,sysdatetimeoffset())
				
				select @p_event_date = CONVERT(varchar(10),@p_event_date_for_autoassign,120)
				select @p_event_hour = substring(CONVERT(varchar(10),@p_event_date_for_autoassign,108),1,2)
				select @p_event_minute = substring(CONVERT(varchar(10),@p_event_date_for_autoassign,108),4,2)
					  
				execute sp_update_call_wfeventverb_status_change @i_client_id,@i_country_code,@i_session_id,@i_user_id,@i_locale_id,@o_service_call_ref_no,'mobile',
					@p_eventverb_id,@p_event_date, @p_event_hour, @p_event_minute,@p_from_wf_stage_no,@p_to_wf_stage_no,@p_from_call_status,@p_to_call_status,@p_by_employee_id,
					@p_assigned_to_emp_id,null,null,'','',@p_inputparam_xml1,'','','<attachment_xml></attachment_xml>',@p_record_timestamp,'A',@p_update_status OUTPUT, @errorNo OUTPUT
				
					
				  if @errorNo != ''
				  begin
					set @errorNo = 'E_UP_073'
					return  
				  end
			end
	  end
end

else if (@i_save_mode = 'U')
begin

 update call_register
 set call_category = @i_call_category,
	 call_type = @i_call_type,
	 customer_id = @i_customer_id,
	 customer_location_code = @i_customer_location_code,
	 organogram_level_no = @i_organogram_level_no,
	 organogram_level_code = @i_organogram_level_code,
	 asset_id = @i_asset_id,
     asset_location_code_reported = @i_asset_location_reported,
     equipment_id = @i_equipment_id,
     problem_description = @i_problem_description,
     additional_information = @i_additional_description,
     priority_code = @i_priority_code,
     customer_contact_name = @i_customer_contact_name,
     customer_contact_no = @i_customer_contact_no,
     customer_contact_email_id = @i_customer_contact_email_id,
     billable_nonbillable_ind = @i_billable_nonbillable_ind,
     charges_currency_code = @i_charges_currency_code,
     charges_gross_amount = @i_charges_gross_amount,
     charges_discount_amount = @i_charges_discount_amount,
     charges_tax_amount = @i_charges_tax_amount,
     charges_net_amount = @i_charges_net_amount,
     call_mapped_to_func_role = @i_call_mapped_to_func_role, 
     call_mapped_to_employee_id = @i_call_mapped_to_employee_id,
     service_contract_doc_no = @i_contract_doc_no, 
     udf_char_1 = case (select applicable from #applicable_custom_fields
						where field_type = 'udf_char_1')
					when 1 then 	isnull( (select paramval from #input_params where paramname = 'udf_char_1') ,'')	
					else NULL
					end,
	 udf_char_2 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_char_2')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_2') ,'')
			else NULL
		   end,
	 udf_char_3 =   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_char_3')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_3') ,'')
			else NULL
		   end,
	 udf_char_4 =	   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_char_4')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_char_4') ,'')
			else NULL
		   end,
	 udf_float_1=	   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_float_1')
			when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_1') ,0)
			else NULL
		   end,
	 udf_float_2=	   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_float_2')
			when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_2') ,'')
			else NULL
		   end,
	udf_float_3=	   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_float_3')
			when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_3') ,0)
			else NULL
		   end,
	udf_float_4 =  case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_float_4')
			when 1 then isnull( (select cast(paramval as float) from #input_params where paramname = 'udf_float_4') ,0)
			else NULL
		   end,
	udf_bit_1 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_bit_1')
			when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_1') ,0)
			else NULL
		   end,
	udf_bit_2 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_bit_2')
			when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_2') ,0)
			else NULL
		   end,
	udf_bit_3= case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_bit_3')
			when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_3') ,0)
			else NULL
		   end,
	udf_bit_4= case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_bit_4')
			when 1 then isnull( (select cast(paramval as bit) from #input_params where paramname = 'udf_bit_4') ,0)
			else NULL
		   end,
	udf_date_1=	   case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_date_1')
			when 1 then 
			isnull( (select CONVERT(datetimeoffset,
			 (select x.paramval from #input_params x where x.paramname = 'udf_date_1')
			+' ' +
			 (select y.paramval from #input_params y where y.paramname = 'udf_date_1_hour')
			 + ':' + 
			 (select z.paramval from #input_params z where z.paramname = 'udf_date_1_minute')
			 +':00',120)), sysdatetimeoffset())
			else NULL
		   end,
	udf_date_2 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_date_2')
			when 1 then 
			isnull( (select CONVERT(datetimeoffset,
			 (select x.paramval from #input_params x where x.paramname = 'udf_date_2')
			+' ' +
			 (select y.paramval from #input_params y where y.paramname = 'udf_date_2_hour')
			 + ':' + 
			 (select z.paramval from #input_params z where z.paramname = 'udf_date_2_minute')
			 +':00',120)), sysdatetimeoffset())
			else NULL
		   end,
	udf_date_3 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_date_3')
			when 1 then
			isnull( (select CONVERT(datetimeoffset,
			 (select x.paramval from #input_params x where x.paramname = 'udf_date_3')
			+' ' +
			 (select y.paramval from #input_params y where y.paramname = 'udf_date_3_hour')
			 + ':' + 
			 (select z.paramval from #input_params z where z.paramname = 'udf_date_3_minute')
			 +':00',120)), sysdatetimeoffset())
			else NULL
		   end,
	udf_date_4 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_date_4')
			when 1 then 
			isnull( (select CONVERT(datetimeoffset,
			 (select x.paramval from #input_params x where x.paramname = 'udf_date_4')
			+' ' +
			 (select y.paramval from #input_params y where y.paramname = 'udf_date_4_hour')
			 + ':' + 
			 (select z.paramval from #input_params z where z.paramname = 'udf_date_4_minute')
			 +':00',120)), sysdatetimeoffset())
				else NULL
		   end,
	udf_analysis_code1 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_analysis_code1')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code1') ,'')
			else NULL
		   end,
	udf_analysis_code2 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_analysis_code2')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code2') ,'')
			else NULL
		   end,
	udf_analysis_code3 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_analysis_code3')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code3') ,'')
			else NULL
		   end,
	udf_analysis_code4 = case (select applicable from #applicable_custom_fields
				 where field_type = 'udf_analysis_code4')
			when 1 then isnull( (select paramval from #input_params where paramname = 'udf_analysis_code4') ,'')
			else NULL
		   end,
     last_update_id = @i_user_id
 where company_id = @i_client_id
   and country_code = @i_country_code
   and call_ref_no = @o_service_call_ref_no
   
  if ( @@ROWCOUNT = 0) 
  begin
	set @errorNo = 'E_UP_073'
	return
  end

       
    /* Include call register dependencies like resource list, part list*/
    if @i_asset_id != 'ZZZ'
    begin
    
		/* If there is a contract for customer and a visit, create a link*/
		if @i_contract_doc_no != ''
		begin
			
			update asset_service_schedule
			set service_visit_status = 'SP',
				call_jo_ind = 'C',
				call_ref_jo_no = @o_service_call_ref_no,
				last_update_id = @i_user_id
			where company_id = @i_client_id
			  and country_code = @i_country_code
			  and asset_id = @i_asset_id
			  and contract_doc_no = @i_contract_doc_no
			  and service_visit_slno = @i_contract_visit_no
	
			if @@ROWCOUNT = 0
			begin
				set @errorNo = 'E_UP_073'
				return
			end        
      		  
		end
		else /* Incase contract doc no is sent blank and if there was a link with service schedule, it should be removed */
		begin
			
			if exists ( select 1 from asset_service_schedule
						where company_id = @i_client_id
						  and country_code = @i_country_code
						  and asset_id = @i_asset_id
						  and call_jo_ind = 'C'
						  and call_ref_jo_no = @o_service_call_ref_no)
			begin
						  
				update asset_service_schedule
				set service_visit_status = null,
					call_jo_ind = null,
					call_ref_jo_no = null,
					last_update_id = @i_user_id
				where company_id = @i_client_id
				  and country_code = @i_country_code
				  and asset_id = @i_asset_id
				  and call_jo_ind = 'C'
				  and call_ref_jo_no = @o_service_call_ref_no
		
				if @@ROWCOUNT = 0
				begin
					set @errorNo = 'E_UP_073'
					return
				end
		   end
		end
		
	end
	  
end

  select @o_update_status = 'SP001'

    SET NOCOUNT OFF;
END
