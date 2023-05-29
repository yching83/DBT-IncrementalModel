{{
    config(
        materialized='incremental',
        key= ['date_day','admin_id_status','row_num']
    )
}}


select 
admin_id
, '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S:%S") }}' as date_day
, admin_status
, admin_created_date
, current_timestamp as run_date
, concat(admin_id, admin_status_string) as admin_id_status
, row_number() over ( order by '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S:%S") }}',admin_id, admin_status_string) as row_num
from 
(select distinct admin_id, admin_status, admin_status_string, admin_created_date from  {{ ref('sales_expansion_model_org') }} )

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where concat(admin_id, admin_status_string) not in (select admin_id_status from {{this}})

{% endif %}
