# mining_commercial_v2 - Databricks Asset Bundle

## Overview
The VP Commercial at Mega Minerals, a global iron ore producer, uses Databricks to unify mine, rail, port, trading, and contract data into a single source of truth. In October–November 2025, a combination of unplanned ship-loader downtime and a spike in freight and FX volatility exposes gaps between physical operations and commercial decisions: port stockpiles for flagship 62% Fe fines fall from ~1.4Mt to ~0.8Mt while three large vessels face demurrage risk, dynamic pricing lags market moves by $4–6/t for a few key customers, and predictive maintenance models show over $25M of revenue at risk if a critical conveyor fails. A GenAI assistant over contracts and ESG PDFs helps the team quickly find which contracts allow price re-openers on carbon taxes and which have Scope 3 reporting obligations, enabling targeted renegotiations and better risk management.

## Deployment

This bundle can be deployed to any Databricks workspace using Databricks Asset Bundles (DAB):

### Prerequisites
1. **Databricks CLI**: Install the latest version
   ```bash
   pip install databricks-cli
   ```
2. **Authentication**: Configure your workspace credentials
   ```bash
   databricks configure
   ```
3. **Workspace Access**: Ensure you have permissions for:
   - Unity Catalog catalog/schema creation
   - SQL Warehouse access
   - Workspace file storage

### Deploy the Bundle
```bash
# Navigate to the dab directory
cd dab/

# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace (--force-lock to override any existing locks)
databricks bundle deploy --force-lock

# Run the data generation workflow
databricks bundle run demo_workflow
```

The deployment will:
1. Create Unity Catalog resources (schema and volume)
2. Upload PDF files to workspace (if applicable)
3. Deploy job and dashboard resources

The workflow will:
1. Create Unity Catalog catalog if it doesn't exist (DAB doesn't support catalog creation)
2. Generate synthetic data using Faker and write to Unity Catalog Volume
3. Execute SQL transformations (bronze → silver → gold)
4. Deploy agent bricks (Genie spaces, Knowledge Assistants, Multi-Agent Supervisors) if configured

## Bundle Contents

### Core Files
- `databricks.yml` - Asset bundle configuration defining jobs, dashboards, and deployment settings
- `bricks_conf.json` - Agent brick configurations (Genie/KA/MAS) if applicable
- `agent_bricks_service.py` - Service for managing agent brick resources (includes type definitions)
- `deploy_resources.py` - Script to recreate agent bricks in the target workspace

### Data Generation
- Python scripts using Faker library for realistic synthetic data
- Configurable row counts, schemas, and business logic
- Automatic Delta table creation in Unity Catalog

### SQL Transformations
- `transformations.sql` - SQL transformations for data processing
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) medallion architecture
- Views and tables for business analytics

### Agent Bricks
This bundle includes AI agent resources:

- **Genie Space** (ID: `01f0dfbb2e011ac2b6f48a6641e3cca4`)
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

### Dashboards
This bundle includes Lakeview dashboards:
- **Mega Minerals Integrated Supply Chain** - Business intelligence dashboard with visualizations

### PDF Documents
No PDF documents are included in this demo.

## Configuration

### Unity Catalog
- **Catalog**: `demo_generator`
- **Schema**: `adrian_tompkins_mining_commercial_v2`
- **Workspace Path**: `/Users/adrian.tompkins@databricks.com/mining_commercial_v2`

### Customization
You can modify the bundle by editing `databricks.yml`:
- Change target catalog/schema in the `variables` section
- Adjust cluster specifications for data generation
- Add additional tasks or resources

## Key Questions This Demo Answers
1. When did MM62 port inventory at the Pilbara terminal first deviate from its usual 0.8–0.9Mt range, and how did that affect our ability to cover scheduled vessels for Dragon Steel and other key customers?
2. Which vessels and customers drove the October demurrage spike to around $1.6M, and how much of that exposure was due to the SL-2 ship-loader outage versus slower rail replenishment?
3. During the October outage, how often did we load ore below contract Fe or above moisture limits, and what penalties or lost bonuses did this create by customer and shipment?
4. Under a scenario where freight rates rise by $3/t and AUD/USD strengthens by 0.02, what happens to quarterly EBITDA and contract margins for our top five iron ore customers, and what price corridor should we target for new MM62 deals?
5. Which critical assets (conveyors, ship loaders, stacker-reclaimers) show the highest revenue at risk over the next 14 days, and what specific shipments and customers would be impacted if they fail?
6. Which current offtake contracts include carbon tax pass-through or price reopener clauses, and among those, what is the current margin and shipment schedule for customers with Scope 3 reporting obligations?
7. How much of our current inventory value is sitting at port versus on water for MM62, and how would a further two-day outage of SL-2 change demurrage exposure and inventory days on hand?
8. For contracts without carbon price reopeners, what is the estimated EBITDA impact if EU carbon costs increase by $20/t on relevant volumes, and which counterparties should we prioritize for renegotiation?

## Deployment to New Workspaces

This bundle is **portable** and can be deployed to any Databricks workspace:

1. The bundle will recreate all resources in the target workspace
2. Agent bricks (Genie/KA/MAS) are recreated from saved configurations in `bricks_conf.json`
3. SQL transformations and data generation scripts are environment-agnostic
4. Dashboards are deployed as Lakeview dashboard definitions

Simply run `databricks bundle deploy` in any workspace where you have the required permissions.

## Troubleshooting

### Common Issues

**Bundle validation fails:**
- Ensure `databricks.yml` has valid YAML syntax
- Check that catalog and schema names are valid
- Verify warehouse lookup matches an existing warehouse

**Agent brick deployment fails:**
- Check that `bricks_conf.json` exists and contains valid configurations
- Ensure you have permissions to create Genie spaces, KA tiles, and MAS tiles
- Verify vector search endpoint exists for Knowledge Assistants

**SQL transformations fail:**
- Ensure the catalog and schema exist in the target workspace
- Check warehouse permissions and availability
- Review SQL syntax for Unity Catalog compatibility (3-level namespace: `catalog.schema.table`)

### Getting Help
- Review Databricks Asset Bundles documentation: https://docs.databricks.com/dev-tools/bundles/
- Check the generated code in this bundle for implementation details
- Contact your Databricks workspace administrator for permissions issues

## Generated with AI Demo Generator
🤖 This bundle was automatically created using the Databricks AI Demo Generator.

**Created**: 2025-12-23 03:43:52
**User**: adrian.tompkins@databricks.com
