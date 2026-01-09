-- =======================================================
-- MEGA MINERALS COMMERCIAL DEMO - SILVER & GOLD LAYER
-- =======================================================
USE CATALOG demo_generator;
USE SCHEMA adrian_tompkins_mining_commercial;
-- =======================================================
-- SILVER TABLES (CLEANED / ENRICHED RAW DATA)
-- =======================================================

-- 1) silver_mine_production
CREATE MATERIALIZED VIEW silver_mine_production 
COMMENT "Clean daily iron ore production by mine and product. Derived from raw_mine_production. Includes standardized enums and a month bucket. Used for upstream supply vs shipments comparisons."
AS
SELECT
  production_id,
  CAST(production_date AS DATE) AS production_date,
  TRIM(mine_site) AS mine_site,
  TRIM(product_code) AS product_code,
  CAST(tonnes_produced AS DOUBLE) AS tonnes_produced,
  DATE_TRUNC('MONTH', CAST(production_date AS DATE)) AS month
FROM raw_mine_production;

-- 2) silver_rail_movements
CREATE MATERIALIZED VIEW silver_rail_movements 
COMMENT "Clean rail movements from mines to ports with derived departure/arrival dates. Used to compute rail inflows and tonnes in transit for vessel coverage."
AS
SELECT
  rail_id,
  CAST(departure_time AS TIMESTAMP) AS departure_time,
  CAST(arrival_time   AS TIMESTAMP) AS arrival_time,
  DATE(CAST(departure_time AS TIMESTAMP)) AS departure_date,
  DATE(CAST(arrival_time   AS TIMESTAMP)) AS arrival_date,
  TRIM(origin_mine) AS origin_mine,
  TRIM(port_site)   AS port_site,
  TRIM(product_code) AS product_code,
  CAST(tonnes_rail AS DOUBLE) AS tonnes_rail
FROM raw_rail_movements;

-- 3) silver_port_stockpile_events
CREATE MATERIALIZED VIEW silver_port_stockpile_events 
COMMENT "Clean stockpile transaction events at ports. Grain: one event with timestamp, site, product, and tonnes_delta. Base fact for computing daily port inventory and ship-loaded tonnes."
AS
SELECT
  event_id,
  CAST(event_time AS TIMESTAMP) AS event_time,
  DATE(CAST(event_time AS TIMESTAMP)) AS event_date,
  TRIM(site) AS site,
  TRIM(stockpile_id) AS stockpile_id,
  TRIM(product_code) AS product_code,
  TRIM(event_type) AS event_type,
  CAST(tonnes_delta AS DOUBLE) AS tonnes_delta,
  shipment_id
FROM raw_port_stockpile_events;

-- 4) silver_vessel_schedule (enriched with contract terms)
CREATE MATERIALIZED VIEW silver_vessel_schedule 
COMMENT "Vessel schedule enriched with matched contract terms (pricing index, freight term, demurrage rules). Grain: one row per vessel. Used for vessel coverage, demurrage, and linking to contracts."
AS
WITH contracts AS (
  SELECT
    contract_id,
    customer_name,
    product_code,
    contract_start_date,
    contract_end_date,
    pricing_index,
    freight_term,
    fx_currency,
    demurrage_free_days,
    demurrage_rate_usd_per_day
  FROM raw_commercial_contracts
)
SELECT
  vs.vessel_id,
  vs.vessel_name,
  TRIM(vs.customer_name) AS customer_name,
  TRIM(vs.product_code) AS product_code,
  TRIM(vs.site) AS site,
  CAST(vs.laycan_start_date AS DATE) AS laycan_start_date,
  CAST(vs.laycan_end_date   AS DATE) AS laycan_end_date,
  CAST(vs.planned_arrival_time AS TIMESTAMP) AS planned_arrival_time,
  CAST(vs.actual_arrival_time  AS TIMESTAMP) AS actual_arrival_time,
  CAST(vs.planned_tonnes AS DOUBLE) AS planned_tonnes,
  CAST(vs.actual_loaded_tonnes AS DOUBLE) AS actual_loaded_tonnes,
  CAST(vs.demurrage_rate_usd_per_day AS DOUBLE) AS vessel_demurrage_rate_usd_per_day,
  c.contract_id,
  c.pricing_index,
  c.freight_term,
  c.fx_currency,
  c.demurrage_free_days,
  COALESCE(c.demurrage_rate_usd_per_day, vs.demurrage_rate_usd_per_day) AS effective_demurrage_rate_usd_per_day,
  DATE_TRUNC('MONTH', CAST(vs.laycan_start_date AS DATE)) AS vessel_month,
  DATEDIFF(CAST(vs.laycan_end_date AS DATE), CAST(vs.laycan_start_date AS DATE)) + 1 AS laycan_days
FROM raw_vessel_schedule vs
LEFT JOIN contracts c
  ON vs.customer_name = c.customer_name
 AND vs.product_code  = c.product_code
 AND CAST(vs.laycan_start_date AS DATE) BETWEEN c.contract_start_date AND c.contract_end_date;

-- 5) silver_ore_quality_assays
CREATE MATERIALIZED VIEW silver_ore_quality_assays 
COMMENT "Clean quality assay results by sample. Includes Fe/moisture/impurities and shipment linkage where available. Used to compare shipped quality to contract specs and compute penalties/bonuses."
AS
SELECT
  assay_id,
  CAST(sample_time AS TIMESTAMP) AS sample_time,
  DATE(CAST(sample_time AS TIMESTAMP)) AS sample_date,
  TRIM(site) AS site,
  TRIM(product_code) AS product_code,
  shipment_id,
  CAST(fe_pct AS DOUBLE) AS fe_pct,
  CAST(moisture_pct AS DOUBLE) AS moisture_pct,
  CAST(sio2_pct AS DOUBLE) AS sio2_pct,
  CAST(al2o3_pct AS DOUBLE) AS al2o3_pct,
  CAST(p_pct AS DOUBLE) AS p_pct
FROM raw_ore_quality_assays;

-- 6) silver_contracts
CREATE MATERIALIZED VIEW silver_contracts 
COMMENT "Master commercial contract dimension with standardized enums and flags. Used across supply chain, pricing, and ESG/GenAI questions."
AS
SELECT
  contract_id,
  TRIM(customer_name) AS customer_name,
  TRIM(product_code) AS product_code,
  CAST(contract_start_date AS DATE) AS contract_start_date,
  CAST(contract_end_date   AS DATE) AS contract_end_date,
  TRIM(pricing_index) AS pricing_index,
  TRIM(freight_term) AS freight_term,
  TRIM(fx_currency) AS fx_currency,
  CAST(fe_min_pct AS DOUBLE) AS fe_min_pct,
  CAST(moisture_max_pct AS DOUBLE) AS moisture_max_pct,
  CAST(has_carbon_price_reopener AS BOOLEAN) AS has_carbon_price_reopener,
  CAST(requires_scope3_reporting AS BOOLEAN) AS requires_scope3_reporting,
  CAST(demurrage_free_days AS INT) AS demurrage_free_days,
  CAST(demurrage_rate_usd_per_day AS DOUBLE) AS demurrage_rate_usd_per_day,
  CAST(base_margin_target_usd_per_t AS DOUBLE) AS base_margin_target_usd_per_t,
  YEAR(contract_start_date) AS contract_year
FROM raw_commercial_contracts;

-- 7) silver_market_prices
CREATE MATERIALIZED VIEW silver_market_prices 
COMMENT "Daily external market indices (62FE, 58FE, 65FE, freight) with month buckets. Used for pricing analytics and inventory valuation."
AS
SELECT
  CAST(price_date AS DATE) AS price_date,
  TRIM(index_name) AS index_name,
  CAST(price_usd_per_t AS DOUBLE) AS price_usd_per_t,
  DATE_TRUNC('MONTH', CAST(price_date AS DATE)) AS month
FROM raw_market_prices;

-- 8) silver_fx_rates
CREATE MATERIALIZED VIEW silver_fx_rates 
COMMENT "Daily FX rates by currency pair (AUDUSD, USDCNY, etc.) with month buckets. Used to translate USD prices into local currencies and in pricing scenarios."
AS
SELECT
  CAST(fx_date AS DATE) AS fx_date,
  TRIM(currency_pair) AS currency_pair,
  CAST(fx_rate AS DOUBLE) AS fx_rate,
  DATE_TRUNC('MONTH', CAST(fx_date AS DATE)) AS month
FROM raw_fx_rates;

-- 9) silver_cost_curves
CREATE MATERIALIZED VIEW silver_cost_curves 
COMMENT "Clean internal cost curves by product, region, and quarter. Provides base cash cost and sensitivities to fuel, freight, and FX for pricing simulations."
AS
SELECT
  cost_curve_id,
  TRIM(product_code) AS product_code,
  TRIM(region) AS region,
  TRIM(quarter) AS quarter,
  CAST(unit_cash_cost_usd_per_t AS DOUBLE) AS unit_cash_cost_usd_per_t,
  CAST(fuel_cost_sensitivity_usd_per_t AS DOUBLE) AS fuel_cost_sensitivity_usd_per_t,
  CAST(freight_cost_sensitivity_usd_per_t AS DOUBLE) AS freight_cost_sensitivity_usd_per_t,
  CAST(fx_sensitivity_usd_per_t AS DOUBLE) AS fx_sensitivity_usd_per_t
FROM raw_cost_curves;

-- 10) silver_contract_positions
CREATE MATERIALIZED VIEW silver_contract_positions 
COMMENT "Contract position facts by quarter and product, joined with contract terms. Used to compute contract-level margins and EBITDA in dynamic pricing."
AS
SELECT
  cp.position_id,
  cp.contract_id,
  TRIM(cp.quarter) AS quarter,
  TRIM(cp.product_code) AS product_code,
  CAST(cp.total_volume_t AS DOUBLE) AS total_volume_t,
  CAST(cp.fixed_price_usd_per_t AS DOUBLE) AS fixed_price_usd_per_t,
  CAST(cp.index_premium_discount_usd_per_t AS DOUBLE) AS index_premium_discount_usd_per_t,
  c.customer_name,
  c.pricing_index,
  c.freight_term,
  c.fx_currency,
  c.base_margin_target_usd_per_t
FROM raw_contract_positions cp
JOIN silver_contracts c
  ON cp.contract_id = c.contract_id;

-- 11) silver_maintenance_logs
CREATE MATERIALIZED VIEW silver_maintenance_logs 
COMMENT "Clean maintenance work orders with computed actual downtime and date. Foundation for asset outage views and predictive maintenance labelling."
AS
SELECT
  work_order_id,
  TRIM(asset_id) AS asset_id,
  TRIM(asset_type) AS asset_type,
  TRIM(site) AS site,
  TRIM(work_order_type) AS work_order_type,
  CAST(start_time AS TIMESTAMP) AS start_time,
  CAST(end_time   AS TIMESTAMP) AS end_time,
  CASE
    WHEN downtime_hours IS NOT NULL THEN CAST(downtime_hours AS DOUBLE)
    WHEN end_time IS NOT NULL AND start_time IS NOT NULL THEN
      (UNIX_TIMESTAMP(CAST(end_time AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(start_time AS TIMESTAMP))) / 3600.0
    ELSE NULL
  END AS actual_downtime_hours,
  DATE(CAST(start_time AS TIMESTAMP)) AS date
FROM raw_maintenance_logs;

-- 12) silver_asset_telemetry
CREATE MATERIALIZED VIEW silver_asset_telemetry 
COMMENT "Daily aggregated telemetry features (utilization, vibration, temperature) per asset. Used as input features for predictive maintenance risk scoring."
AS
SELECT
  TRIM(asset_id) AS asset_id,
  CAST(date AS DATE) AS date,
  CAST(utilization_pct AS DOUBLE) AS utilization_pct,
  CAST(vibration_index AS DOUBLE) AS vibration_index,
  CAST(temperature_index AS DOUBLE) AS temperature_index
FROM raw_asset_telemetry;

-- 13) silver_shipment_revenue
CREATE MATERIALIZED VIEW silver_shipment_revenue 
COMMENT "Shipment-level revenue and tonnes with contract context. Used for revenue-at-risk, quality vs contract, and linking shipments to vessels/customers."
AS
SELECT
  sr.shipment_id,
  sr.contract_id,
  sr.vessel_id,
  TRIM(sr.product_code) AS product_code,
  CAST(sr.nomination_date AS DATE) AS nomination_date,
  CAST(sr.planned_load_date AS DATE) AS planned_load_date,
  CAST(sr.planned_tonnes AS DOUBLE) AS planned_tonnes,
  CAST(sr.realized_price_usd_per_t AS DOUBLE) AS realized_price_usd_per_t,
  CAST(sr.realized_revenue_usd AS DOUBLE) AS realized_revenue_usd,
  DATE_TRUNC('MONTH', CAST(sr.planned_load_date AS DATE)) AS month,
  c.customer_name,
  c.pricing_index,
  c.freight_term,
  c.fx_currency,
  c.has_carbon_price_reopener,
  c.requires_scope3_reporting
FROM raw_shipment_revenue sr
LEFT JOIN silver_contracts c
  ON sr.contract_id = c.contract_id;

-- 14) silver_asset_events
CREATE MATERIALIZED VIEW silver_asset_events 
COMMENT "Event-style representation of maintenance logs, with one row per asset event per day. Used to surface outages like the SL-2 ship loader failure driving October demurrage."
AS
SELECT
  work_order_id,
  date AS event_date,
  asset_id,
  asset_type,
  site,
  work_order_type AS event_type,
  actual_downtime_hours AS downtime_hours,
  CASE
    WHEN work_order_type = 'unplanned' THEN CONCAT('Unplanned downtime on ', asset_id)
    ELSE CONCAT('Planned maintenance on ', asset_id)
  END AS description
FROM silver_maintenance_logs
WHERE work_order_type IN ('planned','unplanned');

-- 15) silver_asset_risk_scores
CREATE MATERIALIZED VIEW silver_asset_risk_scores 
COMMENT "Per-asset daily predictive risk scores (heuristic in this demo) combining telemetry and asset metadata. Outputs 14-day failure probability and expected downtime hours if a failure occurs."
AS
SELECT
  t.asset_id,
  t.date AS evaluation_date,
  COALESCE(m.asset_type, CASE WHEN t.asset_id LIKE 'SL-%' THEN 'ship_loader' WHEN t.asset_id LIKE 'CV-%' THEN 'conveyor' ELSE 'other' END) AS asset_type,
  COALESCE(m.site, 'Pilbara Port') AS site,
  LEAST(1.0,
    GREATEST(0.0,
      0.02
      + 0.004 * (t.utilization_pct - 75.0)
      + 0.03  * (t.vibration_index - 5.0)
    )
  ) AS predicted_failure_prob_14d,
  CASE
    WHEN COALESCE(m.asset_type, '') = 'ship_loader' THEN 36.0 + 2.0 * (t.vibration_index - 5.0)
    WHEN COALESCE(m.asset_type, '') = 'conveyor' THEN 30.0 + 1.5 * (t.vibration_index - 5.0)
    ELSE 24.0 + 1.0 * (t.vibration_index - 5.0)
  END AS predicted_downtime_hours_if_fail,
  'heuristic_v1' AS model_version
FROM silver_asset_telemetry t
LEFT JOIN (
  SELECT DISTINCT asset_id, asset_type, site
  FROM silver_maintenance_logs
) m
  ON t.asset_id = m.asset_id;


-- =======================================================
-- GOLD TABLES (AGGREGATED BUSINESS METRICS)
-- =======================================================

-- A) gold_port_inventory_daily
CREATE MATERIALIZED VIEW gold_port_inventory_daily 
COMMENT "Daily port inventory by site and product with inventory value and days-on-hand. Drives port inventory counters, MM62 trend lines, and inventory value/days-on-hand visuals."
AS
WITH daily AS (
  SELECT
    event_date,
    site,
    product_code,
    SUM(tonnes_delta) AS net_tonnes_delta,
    SUM(CASE WHEN event_type = 'ship_load' THEN -tonnes_delta ELSE 0.0 END) AS ship_load_tonnes
  FROM silver_port_stockpile_events
  GROUP BY event_date, site, product_code
),
inv AS (
  SELECT
    event_date,
    site,
    product_code,
    net_tonnes_delta,
    ship_load_tonnes,
    SUM(net_tonnes_delta) OVER (
      PARTITION BY site, product_code
      ORDER BY event_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_tonnes_on_hand
  FROM daily
),
ship_load_avg AS (
  SELECT
    site,
    product_code,
    event_date,
    ship_load_tonnes,
    AVG(ship_load_tonnes) OVER (
      PARTITION BY site, product_code
      ORDER BY event_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_ship_load_14d
  FROM daily
),
price_map AS (
  SELECT
    mp.price_date,
    mp.index_name,
    mp.price_usd_per_t
  FROM silver_market_prices mp
)
SELECT
  i.event_date AS date,
  i.site,
  i.product_code,
  i.net_tonnes_delta,
  i.cumulative_tonnes_on_hand AS tonnes_on_hand,
  i.cumulative_tonnes_on_hand / 1e6 AS tonnes_on_hand_millions,
  COALESCE(pm.price_usd_per_t, pm62.price_usd_per_t) AS index_price_usd_per_t,
  i.cumulative_tonnes_on_hand * COALESCE(pm.price_usd_per_t, pm62.price_usd_per_t) AS inventory_value_usd,
  CASE
    WHEN sa.avg_ship_load_14d > 0 THEN i.cumulative_tonnes_on_hand / sa.avg_ship_load_14d
    ELSE NULL
  END AS inventory_days_on_hand
FROM inv i
LEFT JOIN ship_load_avg sa
  ON i.site = sa.site
 AND i.product_code = sa.product_code
 AND i.event_date = sa.event_date
LEFT JOIN price_map pm
  ON i.event_date = pm.price_date
 AND pm.index_name = CASE
        WHEN i.product_code = 'MM62' THEN '62FE_CFR'
        WHEN i.product_code = 'MM58' THEN '58FE_CFR'
        WHEN i.product_code = 'MM65' THEN '65FE_CFR'
        ELSE '62FE_CFR'
      END
LEFT JOIN price_map pm62
  ON i.event_date = pm62.price_date
 AND pm62.index_name = '62FE_CFR';

-- B) gold_vessel_coverage
CREATE MATERIALIZED VIEW gold_vessel_coverage 
COMMENT "Per-vessel coverage and demurrage view combining vessel schedule, port inventory, and rail inflows. Drives vessel coverage bars and demurrage exposure analysis."
AS
WITH inv_at_laycan AS (
  SELECT
    site,
    product_code,
    date,
    tonnes_on_hand
  FROM gold_port_inventory_daily
),
rail_during_laycan AS (
  SELECT
    r.port_site AS site,
    r.product_code,
    v.vessel_id,
    SUM(r.tonnes_rail) AS tonnes_in_transit_during_laycan
  FROM silver_rail_movements r
  JOIN silver_vessel_schedule v
    ON r.port_site = v.site
   AND r.product_code = v.product_code
   AND r.arrival_date BETWEEN v.laycan_start_date AND v.laycan_end_date
  GROUP BY r.port_site, r.product_code, v.vessel_id
)
SELECT
  v.vessel_id,
  v.vessel_name,
  v.customer_name,
  v.product_code,
  v.site,
  v.laycan_start_date,
  v.laycan_end_date,
  v.planned_tonnes,
  v.actual_loaded_tonnes,
  COALESCE(i.tonnes_on_hand, 0.0) AS tonnes_on_hand_at_laycan_start,
  COALESCE(rdt.tonnes_in_transit_during_laycan, 0.0) AS tonnes_in_transit_during_laycan,
  LEAST(v.planned_tonnes,
        COALESCE(i.tonnes_on_hand, 0.0) + COALESCE(rdt.tonnes_in_transit_during_laycan, 0.0)
  ) AS covered_tonnes,
  CASE
    WHEN v.planned_tonnes > 0 THEN
      LEAST(1.0,
        LEAST(v.planned_tonnes,
          COALESCE(i.tonnes_on_hand, 0.0) + COALESCE(rdt.tonnes_in_transit_during_laycan, 0.0)
        ) / v.planned_tonnes
      )
    ELSE NULL
  END AS coverage_ratio,
  v.effective_demurrage_rate_usd_per_day AS demurrage_rate_usd_per_day,
  GREATEST(
    0.0,
    DATEDIFF(DATE(v.actual_arrival_time), v.laycan_end_date)
    + CASE WHEN (CASE WHEN v.planned_tonnes > 0 THEN
                        LEAST(v.planned_tonnes,
                          COALESCE(i.tonnes_on_hand, 0.0) + COALESCE(rdt.tonnes_in_transit_during_laycan, 0.0)
                        ) / v.planned_tonnes
                      ELSE 1.0 END) < 0.95
           THEN 1.5 ELSE 0.0 END
  ) AS expected_demurrage_days,
  GREATEST(
    0.0,
    DATEDIFF(DATE(v.actual_arrival_time), v.laycan_end_date)
    + CASE WHEN (CASE WHEN v.planned_tonnes > 0 THEN
                        LEAST(v.planned_tonnes,
                          COALESCE(i.tonnes_on_hand, 0.0) + COALESCE(rdt.tonnes_in_transit_during_laycan, 0.0)
                        ) / v.planned_tonnes
                      ELSE 1.0 END) < 0.95
           THEN 1.5 ELSE 0.0 END
  ) * v.effective_demurrage_rate_usd_per_day AS demurrage_exposure_usd,
  DATE_TRUNC('MONTH', v.laycan_start_date) AS month
FROM silver_vessel_schedule v
LEFT JOIN inv_at_laycan i
  ON v.site = i.site
 AND v.product_code = i.product_code
 AND v.laycan_start_date = i.date
LEFT JOIN rail_during_laycan rdt
  ON v.vessel_id = rdt.vessel_id;

-- C) gold_quality_vs_contract
CREATE MATERIALIZED VIEW gold_quality_vs_contract 
COMMENT "Shipment-level comparison of actual assay quality vs contract specs, with dollar penalty/bonus. Drives Quality vs Contract charts."
AS
WITH shipment_assays AS (
  SELECT
    a.shipment_id,
    ANY_VALUE(a.site) AS site,
    ANY_VALUE(a.product_code) AS product_code,
    AVG(a.fe_pct) AS avg_fe_pct,
    AVG(a.moisture_pct) AS avg_moisture_pct,
    AVG(a.sio2_pct) AS avg_sio2_pct,
    AVG(a.al2o3_pct) AS avg_al2o3_pct,
    AVG(a.p_pct) AS avg_p_pct
  FROM silver_ore_quality_assays a
  WHERE a.shipment_id IS NOT NULL
  GROUP BY a.shipment_id
)
SELECT
  sa.shipment_id,
  sr.vessel_id,
  sr.customer_name,
  sa.site,
  sa.product_code,
  sr.planned_load_date,
  sa.avg_fe_pct,
  sa.avg_moisture_pct,
  sa.avg_sio2_pct,
  sa.avg_al2o3_pct,
  sa.avg_p_pct,
  c.fe_min_pct AS contract_fe_min_pct,
  c.moisture_max_pct AS contract_moisture_max_pct,
  sr.realized_revenue_usd,
  CASE
    WHEN sa.avg_fe_pct IS NOT NULL AND c.fe_min_pct IS NOT NULL AND sa.avg_fe_pct < c.fe_min_pct THEN
      -500000.0 * (c.fe_min_pct - sa.avg_fe_pct) / 0.1
    WHEN sa.avg_fe_pct IS NOT NULL AND c.fe_min_pct IS NOT NULL AND sa.avg_fe_pct > c.fe_min_pct THEN
      300000.0 * (sa.avg_fe_pct - c.fe_min_pct) / 0.1
    ELSE 0.0
  END
  + CASE
      WHEN sa.avg_moisture_pct IS NOT NULL AND c.moisture_max_pct IS NOT NULL AND sa.avg_moisture_pct > c.moisture_max_pct THEN
        -300000.0 * (sa.avg_moisture_pct - c.moisture_max_pct) / 0.1
      ELSE 0.0
    END AS quality_penalty_usd
FROM shipment_assays sa
JOIN silver_shipment_revenue sr
  ON sa.shipment_id = sr.shipment_id
JOIN silver_contracts c
  ON sr.contract_id = c.contract_id;

-- D) gold_supply_chain_financials
CREATE MATERIALIZED VIEW gold_supply_chain_financials 
COMMENT "Monthly rollup of demurrage costs and inventory metrics by product. Used for monthly demurrage vs inventory days-on-hand views."
AS
WITH dem AS (
  SELECT
    DATE_TRUNC('MONTH', laycan_start_date) AS month,
    product_code,
    SUM(demurrage_exposure_usd) AS total_demurrage_usd
  FROM gold_vessel_coverage
  GROUP BY DATE_TRUNC('MONTH', laycan_start_date), product_code
),
inv AS (
  SELECT
    DATE_TRUNC('MONTH', date) AS month,
    site,
    product_code,
    AVG(inventory_days_on_hand) AS avg_inventory_days_on_hand,
    AVG(inventory_value_usd) AS avg_inventory_value_usd
  FROM gold_port_inventory_daily
  GROUP BY DATE_TRUNC('MONTH', date), site, product_code
),
water AS (
  SELECT
    DATE_TRUNC('MONTH', laycan_start_date) AS month,
    site,
    product_code,
    SUM(planned_tonnes * COALESCE(NULLIF(coverage_ratio,0),1.0) * 0.5) AS tonnes_on_water_equiv
  FROM gold_vessel_coverage
  GROUP BY DATE_TRUNC('MONTH', laycan_start_date), site, product_code
)
SELECT
  d.month,
  d.product_code,
  COALESCE(d.total_demurrage_usd, 0.0) AS total_demurrage_usd,
  i.avg_inventory_days_on_hand AS avg_inventory_days_on_hand,
  i.avg_inventory_value_usd AS inventory_value_port_usd,
  w.tonnes_on_water_equiv AS tonnes_on_water_equiv
FROM dem d
LEFT JOIN inv i
  ON d.month = i.month
 AND d.product_code = i.product_code
 AND i.site = 'Pilbara Port'
LEFT JOIN water w
  ON d.month = w.month
 AND d.product_code = w.product_code
 AND w.site = 'Pilbara Port';

-- E) gold_asset_events
CREATE MATERIALIZED VIEW gold_asset_events 
COMMENT "Event log of planned and unplanned maintenance events by asset. Highlights key outages driving port constraints."
AS
SELECT
  event_date,
  asset_id,
  asset_type,
  site,
  event_type,
  downtime_hours,
  description
FROM silver_asset_events;

-- F) gold_market_pricing
CREATE MATERIALIZED VIEW gold_market_pricing 
COMMENT "Daily market pricing layer linking iron ore indices, freight, and AUDUSD FX. Provides USD and AUD prices for scenario modeling."
AS
SELECT
  mp.price_date,
  mp.index_name,
  mp.price_usd_per_t,
  fx_AUDUSD.fx_rate AS audusd,
  CASE WHEN mp.index_name = '62FE_CFR' THEN mp.price_usd_per_t / fx_AUDUSD.fx_rate ELSE NULL END AS price_aud_per_t,
  CASE WHEN mp.index_name = 'PLATTS_WA_CHINA_FREIGHT' THEN mp.price_usd_per_t ELSE NULL END AS freight_usd_per_t
FROM silver_market_prices mp
LEFT JOIN silver_fx_rates fx_AUDUSD
  ON mp.price_date = fx_AUDUSD.fx_date
 AND fx_AUDUSD.currency_pair = 'AUDUSD';

-- G) gold_pricing_positions
CREATE MATERIALIZED VIEW gold_pricing_positions 
COMMENT "Contract position gold layer: margin per tonne and EBITDA impact under base case and stressed scenarios. Used in Dynamic Pricing notebook."
AS
WITH index_by_quarter AS (
  SELECT
    DATE_TRUNC('QUARTER', price_date) AS quarter_start,
    index_name,
    AVG(price_usd_per_t) AS avg_price_usd_per_t
  FROM gold_market_pricing
  GROUP BY DATE_TRUNC('QUARTER', price_date), index_name
),
fx_by_quarter AS (
  SELECT
    DATE_TRUNC('QUARTER', fx_date) AS quarter_start,
    AVG(CASE WHEN currency_pair = 'AUDUSD' THEN fx_rate END) AS avg_audusd
  FROM silver_fx_rates
  GROUP BY DATE_TRUNC('QUARTER', fx_date)
)
SELECT
  cp.position_id,
  cp.contract_id,
  cp.customer_name,
  cp.product_code,
  cp.quarter,
  cp.total_volume_t,
  cp.fixed_price_usd_per_t,
  cp.index_premium_discount_usd_per_t,
  cp.pricing_index,
  cp.freight_term,
  cp.fx_currency,
  cc.region,
  cc.unit_cash_cost_usd_per_t,
  cc.fuel_cost_sensitivity_usd_per_t,
  cc.freight_cost_sensitivity_usd_per_t,
  cc.fx_sensitivity_usd_per_t,
  ibq.avg_price_usd_per_t AS index_avg_price_usd_per_t,
  fbq.avg_audusd,
  CASE WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN 'fixed' ELSE 'index_linked' END AS price_type,
  CASE
    WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN cp.fixed_price_usd_per_t
    ELSE ibq.avg_price_usd_per_t + cp.index_premium_discount_usd_per_t
  END AS base_realized_price_usd_per_t,
  CASE
    WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN cp.fixed_price_usd_per_t - cc.unit_cash_cost_usd_per_t
    ELSE (ibq.avg_price_usd_per_t + cp.index_premium_discount_usd_per_t) - cc.unit_cash_cost_usd_per_t
  END AS base_case_margin_usd_per_t,
  CASE
    WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN cp.fixed_price_usd_per_t
    ELSE ibq.avg_price_usd_per_t + cp.index_premium_discount_usd_per_t
  END
  - (cc.unit_cash_cost_usd_per_t
     + cc.fuel_cost_sensitivity_usd_per_t * 1.5
     + cc.freight_cost_sensitivity_usd_per_t * 3.0
     + cc.fx_sensitivity_usd_per_t * 2.0
    ) AS scenario_margin_usd_per_t,
  ((CASE
      WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN cp.fixed_price_usd_per_t
      ELSE ibq.avg_price_usd_per_t + cp.index_premium_discount_usd_per_t
    END
    - (cc.unit_cash_cost_usd_per_t
       + cc.fuel_cost_sensitivity_usd_per_t * 1.5
       + cc.freight_cost_sensitivity_usd_per_t * 3.0
       + cc.fx_sensitivity_usd_per_t * 2.0))
   - (CASE
        WHEN cp.fixed_price_usd_per_t IS NOT NULL THEN cp.fixed_price_usd_per_t - cc.unit_cash_cost_usd_per_t
        ELSE (ibq.avg_price_usd_per_t + cp.index_premium_discount_usd_per_t) - cc.unit_cash_cost_usd_per_t
      END)
  ) * cp.total_volume_t AS ebitda_impact_usd
FROM silver_contract_positions cp
JOIN silver_cost_curves cc
  ON cp.product_code = cc.product_code
 AND cc.region = 'Pilbara'
 AND cp.quarter = cc.quarter
LEFT JOIN index_by_quarter ibq
  ON ibq.quarter_start = CASE
       WHEN cp.quarter = '2025Q3' THEN DATE '2025-07-01'
       WHEN cp.quarter = '2025Q4' THEN DATE '2025-10-01'
       ELSE DATE '2025-07-01'
     END
 AND ibq.index_name = cp.pricing_index
LEFT JOIN fx_by_quarter fbq
  ON fbq.quarter_start = CASE
       WHEN cp.quarter = '2025Q3' THEN DATE '2025-07-01'
       WHEN cp.quarter = '2025Q4' THEN DATE '2025-10-01'
       ELSE DATE '2025-07-01'
     END;

-- H) gold_asset_risk_14d
CREATE MATERIALIZED VIEW gold_asset_risk_14d 
COMMENT "Per-asset risk metrics linking predictive failure probabilities to shipments and revenue. Supports revenue-at-risk visualizations."
AS
WITH shipments_window AS (
  SELECT
    s.shipment_id,
    s.vessel_id,
    s.customer_name,
    s.product_code,
    s.planned_load_date,
    s.planned_tonnes,
    s.realized_revenue_usd,
    vs.site
  FROM silver_shipment_revenue s
  LEFT JOIN silver_vessel_schedule vs
    ON s.vessel_id = vs.vessel_id
),
asset_map AS (
  SELECT DISTINCT
    asset_id,
    asset_type,
    site
  FROM silver_maintenance_logs
)
SELECT
  rs.asset_id,
  rs.asset_type,
  rs.site,
  rs.evaluation_date,
  rs.predicted_failure_prob_14d,
  rs.predicted_downtime_hours_if_fail * rs.predicted_failure_prob_14d AS expected_downtime_hours,
  COUNT(DISTINCT sw.shipment_id) AS total_shipments_at_risk,
  COALESCE(SUM(sw.planned_tonnes), 0.0) AS tonnes_at_risk,
  COALESCE(SUM(sw.realized_revenue_usd), 0.0) * rs.predicted_failure_prob_14d AS revenue_at_risk_usd
FROM silver_asset_risk_scores rs
LEFT JOIN asset_map am
  ON rs.asset_id = am.asset_id
LEFT JOIN shipments_window sw
  ON sw.site = rs.site
 AND sw.planned_load_date BETWEEN rs.evaluation_date AND DATE_ADD(rs.evaluation_date, 14)
 AND (
      (rs.asset_type = 'ship_loader')
      OR (rs.asset_type = 'conveyor')
      OR (rs.asset_type = 'stacker_reclaimer')
     )
GROUP BY
  rs.asset_id,
  rs.asset_type,
  rs.site,
  rs.evaluation_date,
  rs.predicted_failure_prob_14d,
  rs.predicted_downtime_hours_if_fail;

-- I) gold_asset_top_risk_view
CREATE MATERIALIZED VIEW gold_asset_top_risk_view 
COMMENT "Simplified view of the top 10 assets by revenue-at-risk for the latest 14-day window. Used in maintenance dashboards."
AS
WITH anchor AS (
  SELECT MAX(evaluation_date) AS max_eval_date FROM gold_asset_risk_14d
),
windowed AS (
  SELECT *
  FROM gold_asset_risk_14d, anchor
  WHERE evaluation_date BETWEEN DATE_SUB(max_eval_date, 13) AND max_eval_date
),
ranked AS (
  SELECT
    evaluation_date,
    asset_id,
    asset_type,
    site,
    predicted_failure_prob_14d,
    expected_downtime_hours,
    total_shipments_at_risk,
    tonnes_at_risk,
    revenue_at_risk_usd,
    ROW_NUMBER() OVER (PARTITION BY evaluation_date ORDER BY revenue_at_risk_usd DESC) AS rn
  FROM windowed
)
SELECT
  evaluation_date,
  asset_id,
  asset_type,
  site,
  predicted_failure_prob_14d,
  expected_downtime_hours,
  total_shipments_at_risk,
  tonnes_at_risk,
  revenue_at_risk_usd
FROM ranked
WHERE rn <= 10;

-- J) gold_contract_esg_summary
CREATE MATERIALIZED VIEW gold_contract_esg_summary 
COMMENT "Per-contract ESG/commercial summary with carbon price reopener and Scope 3 reporting flags. Used by Genie and ESG assistants."
AS
SELECT
  contract_id,
  customer_name,
  product_code,
  contract_start_date,
  contract_end_date,
  pricing_index,
  freight_term,
  fx_currency,
  has_carbon_price_reopener,
  requires_scope3_reporting,
  base_margin_target_usd_per_t,
  CASE
    WHEN customer_name IN ('EuroSteel','Other Europe') THEN 'EU'
    WHEN customer_name IN ('Dragon Steel','Nippon Metals','Other Asia') THEN 'Asia'
    ELSE 'Other'
  END AS region_tag
FROM silver_contracts;

-- K) gold_genie_semantic_layer
CREATE MATERIALIZED VIEW gold_genie_semantic_layer 
COMMENT "Cross-domain semantic layer for Genie/LLM access. Integrates supply chain, pricing, risk, and ESG metrics into a unified grain."
AS
SELECT
  'port_inventory' AS record_type,
  CAST(NULL AS STRING) AS key_id,
  date,
  site,
  product_code,
  NULL AS customer_name,
  NULL AS contract_id,
  tonnes_on_hand AS metric_value_1,
  inventory_value_usd AS metric_value_2,
  inventory_days_on_hand AS metric_value_3,
  NULL AS metric_value_4,
  NULL AS metric_value_5
FROM gold_port_inventory_daily
UNION ALL
SELECT
  'vessel_coverage' AS record_type,
  vessel_id AS key_id,
  laycan_start_date AS date,
  site,
  product_code,
  customer_name,
  NULL AS contract_id,
  covered_tonnes AS metric_value_1,
  demurrage_exposure_usd AS metric_value_2,
  coverage_ratio AS metric_value_3,
  planned_tonnes AS metric_value_4,
  NULL AS metric_value_5
FROM gold_vessel_coverage
UNION ALL
SELECT
  'asset_risk' AS record_type,
  asset_id AS key_id,
  evaluation_date AS date,
  site,
  NULL AS product_code,
  NULL AS customer_name,
  NULL AS contract_id,
  revenue_at_risk_usd AS metric_value_1,
  predicted_failure_prob_14d AS metric_value_2,
  expected_downtime_hours AS metric_value_3,
  total_shipments_at_risk AS metric_value_4,
  tonnes_at_risk AS metric_value_5
FROM gold_asset_risk_14d
UNION ALL
SELECT
  'pricing_position' AS record_type,
  contract_id AS key_id,
  CASE WHEN quarter = '2025Q3' THEN DATE '2025-07-01' ELSE DATE '2025-10-01' END AS date,
  NULL AS site,
  product_code,
  customer_name,
  contract_id,
  base_case_margin_usd_per_t AS metric_value_1,
  scenario_margin_usd_per_t AS metric_value_2,
  ebitda_impact_usd AS metric_value_3,
  total_volume_t AS metric_value_4,
  NULL AS metric_value_5
FROM gold_pricing_positions
UNION ALL
SELECT
  'contract_esg' AS record_type,
  contract_id AS key_id,
  contract_start_date AS date,
  NULL AS site,
  product_code,
  customer_name,
  contract_id,
  CAST(has_carbon_price_reopener AS DOUBLE) AS metric_value_1,
  CAST(requires_scope3_reporting AS DOUBLE) AS metric_value_2,
  base_margin_target_usd_per_t AS metric_value_3,
  NULL AS metric_value_4,
  NULL AS metric_value_5
FROM gold_contract_esg_summary;