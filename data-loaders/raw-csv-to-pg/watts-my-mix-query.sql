-- Table to store mix data
create table staging.mix_data_test
(
    balancing_authority varchar(4),
    energy_source varchar(4), -- BA where energy is from
    utc_end_time timestamp,
    net_generation_mw real,
    total_interchange_mw real, -- TODO: Want this?
    net_generation_coal_mw real,
    net_generation_natural_gas_mw real,
    net_generation_nuclear_mw real,
    net_generation_petroleum_mw real,
    net_generation_hydropower_mw real,
    net_generation_solar_mw real,
    net_generation_wind_mw real,
    net_generation_other_mw real,
    net_generation_unknown_mw real
);

-- Insert #1 - Own generation
insert into staging.mix_data_test (
    balancing_authority,
    energy_source,
    utc_end_time,
    net_generation_mw,
    total_interchange_mw, -- TODO: Want this?
    net_generation_coal_mw,
    net_generation_natural_gas_mw,
    net_generation_nuclear_mw,
    net_generation_petroleum_mw,
    net_generation_hydropower_mw,
    net_generation_solar_mw,
    net_generation_wind_mw,
    net_generation_other_mw,
    net_generation_unknown_mw
)
select balancing_authority
    , balancing_authority -- for self
    , utc_end_time
    , net_generation_mw
    , total_interchange_mw
    , net_generation_coal_mw
    , net_generation_natural_gas_mw -- 0
    , net_generation_nuclear_mw     -- 0
    , net_generation_petroleum_mw   -- 0
    , net_generation_hydropower_mw
    , net_generation_solar_mw
    , net_generation_wind_mw
    , net_generation_other_mw
    , net_generation_unknown_mw
from staging.energy_balance
where balancing_authority = 'SCL'
    and utc_end_time between '2020-03-01T00:00:00' and '2020-03-08T00:00:00'
;

-- Insert #2 - Energy FROM other BAs
insert into staging.mix_data_test (
    balancing_authority,
    energy_source,
    utc_end_time,
    net_generation_mw,
    total_interchange_mw, -- TODO: Want this?
    net_generation_coal_mw,
    net_generation_natural_gas_mw,
    net_generation_nuclear_mw,
    net_generation_petroleum_mw,
    net_generation_hydropower_mw,
    net_generation_solar_mw,
    net_generation_wind_mw,
    net_generation_other_mw,
    net_generation_unknown_mw
)
select balancing_authority
    , connected_balancing_authority
    , utc_end_time
    , net_generation_mw
    -- TODO: Some BA's didn't report 'interchange_amount_mw' - need to compute for them
    , interchange_amount_mw
    -- , (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_portion
    -- TODO: Some BA's didn't report 'net_generation_adjusted_mw' - need to provide for them
    -- , net_generation_adjusted_mw
    , net_generation_coal_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_coal_mw
    , net_generation_natural_gas_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_natural_gas_mw
    , net_generation_nuclear_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_nuclear_mw
    , net_generation_petroleum_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_petroleum_mw
    , net_generation_hydropower_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_hydropower_mw
    , net_generation_solar_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_solar_mw
    , net_generation_wind_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_wind_mw
    , net_generation_other_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_other_mw
    , net_generation_unknown_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_unknown_mw
from energy_interchange_with_balance_view
where balancing_authority = 'SCL'
    and utc_end_time between '2020-03-01T00:00:00' and '2020-03-08T00:00:00'
    and interchange_amount_mw < 0
;

-- Insert #3 - Energy TO other BAs
-- TODO: Can skip "sent_percentages" CTE, and just query energy_interchange_with_balance_view
-- TODO: What if interchange is 0? Just skip it?

/*
    Final query - gets energy mix for a given BA at a given point-in-time
        - SCL @ 2020-03-03T07:00:00
        -- TODO: How would I persist this? (in case wanted to query total hydro over time)
*/
with sent_percentages as (
    select balancing_authority
        , utc_end_time
        , connected_balancing_authority
        , interchange_amount_mw
    from energy_interchange_with_balance_view
    where balancing_authority = 'SCL'
        and utc_end_time = '2020-03-03T07:00:00'
        and interchange_amount_mw > 0
)
-- 1) get BA's own generation
select balancing_authority          as source
    , net_generation_mw
    , net_generation_coal_mw
    , net_generation_natural_gas_mw -- 0
    , net_generation_nuclear_mw     -- 0
    , net_generation_petroleum_mw   -- 0
    , net_generation_hydropower_mw
    , net_generation_solar_mw
    , net_generation_wind_mw
    , net_generation_other_mw
    , net_generation_unknown_mw
from staging.energy_balance
where balancing_authority = 'SCL'
    and utc_end_time = '2020-03-03T07:00:00'
union
-- 2) Received generation from other BA's
select connected_balancing_authority
    , abs(interchange_amount_mw)
    -- , (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_portion
    -- , net_generation_adjusted_mw
    , net_generation_coal_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_coal_mw
    , net_generation_natural_gas_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_natural_gas_mw
    , net_generation_nuclear_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_nuclear_mw
    , net_generation_petroleum_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_petroleum_mw
    , net_generation_hydropower_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_hydropower_mw
    , net_generation_solar_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_solar_mw
    , net_generation_wind_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_wind_mw
    , net_generation_other_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_other_mw
    , net_generation_unknown_mw * (abs(interchange_amount_mw) / net_generation_adjusted_mw) as interchanged_unknown_mw
from energy_interchange_with_balance_view
where balancing_authority = 'SCL'
    and utc_end_time = '2020-03-03T07:00:00'
    and interchange_amount_mw < 0
union
-- 3) Account for energy sent to other BA's
select sent_percentages.connected_balancing_authority
    --,  (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw) as interchanged_portion
    , -interchange_amount_mw as interchange_amount_mw
    , -(net_generation_coal_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_coal_mw
    , -(net_generation_natural_gas_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_natural_gas_mw
    , -(net_generation_nuclear_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_nuclear_mw
    , -(net_generation_petroleum_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_petroleum_mw
    , -(net_generation_hydropower_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_hydropower_mw
    , -(net_generation_solar_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_solar_mw
    , -(net_generation_wind_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_wind_mw
    , -(net_generation_other_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_other_mw
    , -(net_generation_unknown_mw * (abs(sent_percentages.interchange_amount_mw) / energy_balance.net_generation_adjusted_mw)) as interchanged_unknown_mw
from sent_percentages
inner join staging.energy_balance as energy_balance
    on sent_percentages.balancing_authority = energy_balance.balancing_authority
    and sent_percentages.utc_end_time = energy_balance.utc_end_time
;