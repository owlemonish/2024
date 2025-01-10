with productivity as (
    select fp.sla
        , dp.culturist_id
        , fp.id
        , fp.lead_id  
        , fp.pond_name
        , fp.pond_uuid 
        , fp.start_cultivation_date 
        , fp.doc_until_sample_receipt
        , fp.measurement_date
        , fp.update_date
        , dp.culturist_province_name
        , fp.input_toolsbud_date
        , fp.created_by 
        , case 
            when pks.pks_type in ('CONTRACT_FARMING_RENEWAL', 'CONTRACT_FARMING', 'CFSHRIMP') THEN 1
            else 0 
        end as is_cf
        , CASE 
            WHEN dp.culturist_province_name IN ('ACEH', 'SUMATERA UTARA', 'KEPULAUAN RIAU', 'MEDAN', 'RIAU') THEN 'R1'
            WHEN dp.culturist_province_name IN ('LAMPUNG', 'KEPULAUAN BANGKA BELITUNG') THEN 'R2'
            WHEN dp.culturist_province_name IN ('BANTEN', 'JAWA BARAT') THEN 'R3'
            WHEN dp.culturist_province_name = 'JAWA TENGAH' THEN 'R4'
            WHEN dp.culturist_province_name IN ('JAWA TIMUR', 'BALI', 'NUSA TENGGARA BARAT') THEN 'R5'
            WHEN dp.culturist_province_name IN ('KALIMANTAN BARAT', 'KALIMANTAN TENGAH', 'SULAWESI TENGAH', 'SULAWESI SELATAN', 'SULAWESI BARAT', 'SULAWESI TENGGARA') THEN 'R6'
        ELSE '-'
        END AS region
    from mart.mart_cultivation_field_productivity fp 
    left join warehouse.dim_partner dp 
        on dp.culturist_id = fp.lead_id
    left join warehouse.dim_pks pks 
        on pks.lead_id = fp.lead_id 
  where 1=1
    [[and input_toolsbud_date IN (
        select DISTINCT input_toolsbud_date 
        from `mart.mart_cultivation_field_productivity` 
        where {{input_toolsbud_date}}
    )]]
    [[and fp.created_by IN (
        select DISTINCT created_by 
        from `mart.mart_cultivation_field_productivity` 
        where {{created_by}}
    )]]
),

avg_sla_cf as (
select created_by
    , is_cf 
    , avg(sla) as avg_sla_cf 
from productivity
where is_cf = 1 
[[and CAST(is_cf as string) = {{is_cf}}]]
group by 1,2 
),

avg_sla_non_cf as (
select created_by
    , is_cf 
    , avg(sla) as avg_sla_non_cf 
from productivity
where is_cf = 0
[[and CAST(is_cf as string) = {{is_cf}}]]
group by 1,2 
),

total_avg_sla as (
select created_by
    , avg(sla) as avg_sla 
from productivity
where 1=1
[[and CAST(is_cf as string) = {{is_cf}}]]
group by 1 
),

total_farmer as (
select created_by
    , region 
    , count (distinct lead_id) as total_farmer
    , count (distinct id) as total_sample 
from productivity 
where 1=1 
[[and CAST(is_cf as string) = {{is_cf}}]]
group by 1,2 
order by 2,3 desc 
)

select a.*
    , b.avg_sla 
    , c.avg_sla_cf 
    , d.avg_sla_non_cf 
from total_farmer a 
left join total_avg_sla b 
    on b.created_by = a.created_by
left join avg_sla_cf c 
    on c.created_by = a.created_by
left join avg_sla_non_cf d 
    on d.created_by = c.created_by
where 1=1 
[[and region in unnest(split({{region}}))]]
--[[and CAST(is_cf as string) = {{is_cf}}]]