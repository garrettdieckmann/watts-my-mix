-- !!! TODO: Anywhere we select demand/generation, should be using "adjusted" amount instead
/**
    Create our Watt's My Mix database
**/
create database watts_my_mix;

/**
    Create staging schema
**/
create schema staging;

/**
    Create emissions from energy source table, and pre-populate emission values
**/
-- TODO: Include lifecycle amounts
create table if not exists public.dim_emission_estimates
(
    source varchar(20),
    lbs_co2_per_kwh integer,
    lbs_co2_per_mwh integer
);
insert into public.dim_emission_estimates
values ('coal', 2.23, 2230),
    ('natural_gas', 0.91, 910),
    ('nuclear', 0, 0),
    ('petroleum', 2.13, 2130),
    ('hydropower', 0, 0),
    ('solar', 0, 0),
    ('wind', 0, 0),
    ('other', 0, 0),
    ('unknown', 0, 0)
;

/**
    Create final energy balance hypertable with appropriate column data types
**/
create table public.energy_balance
(
    balancing_authority varchar(4),
    utc_end_time timestamp,
    demand_forecast_mw integer,
    demand_mw integer,
    net_generation_mw integer,
    total_interchange_mw integer,
    demand_adjusted_mw integer,
    net_generation_adjusted_mw integer,
    combined_net_generation_mw integer, -- Sum up individual energy sectors to compute own net generation amount
    net_generation_coal_mw integer,
    net_generation_natural_gas_mw integer,
    net_generation_nuclear_mw integer,
    net_generation_petroleum_mw integer,
    net_generation_hydropower_mw integer,
    net_generation_solar_mw integer,
    net_generation_wind_mw integer,
    net_generation_other_mw integer,
    net_generation_unknown_mw integer
);

-- Create BRIN index
create index energy_balance_brin_idx on energy_balance using brin (balancing_authority, utc_end_time);

/**
    Create final energy interchange hypertable with appropriate column data types
**/
create table public.energy_interchange
(
    balancing_authority varchar(4),
    utc_end_time timestamp,
    connected_balancing_authority varchar(4),
    interchange_amount_mw integer
);

-- Create BRIN index
create index energy_interchange_brin_idx on energy_interchange using brin (balancing_authority, utc_end_time);

/**
    Create denormalized view that includes the energy generation balance
        for each interchange measure
**/
create view public.energy_interchange_with_balance_view
as
select interchange.balancing_authority
    , interchange.utc_end_time
    , interchange.connected_balancing_authority
    , interchange.interchange_amount_mw -- Interchange amount from Balancing Authority (not Connected Authority)
    -- Electricity generation values below are reported from the Connected Authority
    , balance.net_generation_mw
    , balance.net_generation_adjusted_mw
    , balance.combined_net_generation_mw
    , balance.net_generation_coal_mw
    , balance.net_generation_natural_gas_mw
    , balance.net_generation_nuclear_mw
    , balance.net_generation_petroleum_mw
    , balance.net_generation_hydropower_mw
    , balance.net_generation_solar_mw
    , balance.net_generation_wind_mw
    , balance.net_generation_other_mw
    , balance.net_generation_unknown_mw
from energy_interchange as interchange
inner join energy_balance as balance
    on interchange.connected_balancing_authority = balance.balancing_authority
    and interchange.utc_end_time = balance.utc_end_time
;

/*
    Create the energy_balance staging table
*/
create table staging.energy_balance
(
    balancing_authority varchar(4),
    utc_end_time timestamp,
    demand_forecast_mw integer,
    demand_mw integer,
    net_generation_mw integer,
    total_interchange_mw integer,
    demand_adjusted_mw integer,
    net_generation_adjusted_mw integer,
    combined_net_generation_mw integer, -- Sum up individual energy sectors to compute own net generation amount
    net_generation_coal_mw integer,
    net_generation_natural_gas_mw integer,
    net_generation_nuclear_mw integer,
    net_generation_petroleum_mw integer,
    net_generation_hydropower_mw integer,
    net_generation_solar_mw integer,
    net_generation_wind_mw integer,
    net_generation_other_mw integer,
    net_generation_unknown_mw integer
);

-- Create BRIN index
create index energy_balance_brin_idx on staging.energy_balance using brin (balancing_authority, utc_end_time);

/*
    Create the energy_interchange staging table
*/
create table staging.energy_interchange
(
    balancing_authority varchar(4),
    utc_end_time timestamp,
    connected_balancing_authority varchar(4),
    interchange_amount_mw integer
);

-- Create BRIN index
create index energy_interchange_brin_idx
on staging.energy_interchange
using brin (balancing_authority, connected_balancing_authority, utc_end_time);

-------- Below are recurring loading steps ------------------


/**
    Insert from new rows (handling duplicates) from energy_balance staging table into energy_balance destination table
**/
insert into energy_balance
select staging.balancing_authority
    , staging.utc_end_time
    , staging.demand_forecast_mw
    , staging.demand_mw
    , staging.net_generation_mw
    , staging.total_interchange_mw
    , staging.demand_adjusted_mw
    , staging.net_generation_adjusted_mw
    , staging.combined_net_generation_mw
    , staging.net_generation_coal_mw
    , staging.net_generation_natural_gas_mw
    , staging.net_generation_nuclear_mw
    , staging.net_generation_petroleum_mw
    , staging.net_generation_hydropower_mw
    , staging.net_generation_solar_mw
    , staging.net_generation_wind_mw
    , staging.net_generation_other_mw
    , staging.net_generation_unknown_mw
from staging.energy_balance as staging
left join energy_balance
    on staging.balancing_authority = energy_balance.balancing_authority
    and staging.utc_end_time = energy_balance.utc_end_time
where energy_balance.balancing_authority is null
    and energy_balance.utc_end_time is null
;

/**
    Insert from energy_interchange staging table into energy_interchange transformed table
**/
insert into energy_interchange
select staging.balancing_authority
    , staging.utc_end_time
    , staging.connected_balancing_authority
    , staging.interchange_amount_mw
from staging.energy_interchange as staging
left join energy_interchange
    on staging.balancing_authority = energy_interchange.balancing_authority
    and staging.utc_end_time = energy_interchange.utc_end_time
    and staging.connected_balancing_authority = energy_interchange.connected_balancing_authority
where energy_interchange.balancing_authority is null
    and energy_interchange.utc_end_time is null
    and energy_interchange.connected_balancing_authority is null

-- TODO: Insert into statements allow dups into final tables (if dups are already in staging). Should de-dup on insert