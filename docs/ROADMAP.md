# FitTrack Pro Pipeline -- Roadmap

## Current State (v1.0)

The pipeline is fully functional with all core components:

- **Bronze Layer**: File ingestion (CSV/JSON) with metadata tracking and content hashing
- **Silver Layer**: Data cleaning, source-specific transformation, and enrichment
- **Gold Layer**: DuckDB warehouse with star schema, SCD Type 2, dimension tables
- **Quality Engine**: Profiling, validation, anomaly detection, scoring (0-100), reporting
- **Monitoring**: Health checks, JSON alerting, Streamlit dashboard
- **Testing**: 110 tests across ingestion, transformation, quality, and E2E integration
- **Documentation**: 8 guides covering architecture, pipeline, quality, monitoring, and more

---

## Phase 1: API Integration (Next)

### Wger Fitness API (Live Exercise Data)
- [ ] Connect to the live Wger API at `https://wger.de/api/v2/`
- [ ] Implement paginated exercise fetching (offset-based pagination)
- [ ] Pull muscle group and equipment reference data
- [ ] Add rate limiting (respect Wger's 100 requests/minute limit)
- [ ] Schedule daily incremental syncs via the scheduler
- [ ] Add integration tests with recorded API responses (VCR pattern)

### USDA FoodData Central (Nutrition Lookup)
- [ ] Connect to USDA API at `https://api.nal.usda.gov/fdc/v1/`
- [ ] Implement food search endpoint integration
- [ ] Flatten nested nutrient arrays into Silver schema
- [ ] Cache API responses to reduce redundant calls
- [ ] Map USDA nutrients to our nutrition_logs schema

**Why this matters**: The file-based pipeline proves the architecture works. Adding live API sources demonstrates real-world data integration with retry logic, pagination, and rate limiting.

---

## Phase 2: Pipeline Hardening

### Error Recovery
- [ ] Implement dead letter queue for failed records (don't lose data)
- [ ] Add automatic retry for transient failures
- [ ] Create error reporting dashboard in Streamlit
- [ ] Implement circuit breaker pattern for API sources

### Idempotency
- [ ] Add content-based deduplication using `_source_hash`
- [ ] Implement upsert logic for Gold tables (INSERT ON CONFLICT)
- [ ] Track processed file hashes to prevent re-ingestion
- [ ] Add watermark tracking for incremental API pulls

### Configuration
- [ ] Add environment-specific configs (dev/staging/production)
- [ ] Implement secret management (API keys via environment variables)
- [ ] Add feature flags for enabling/disabling pipeline components
- [ ] Create a config validation script

**Why this matters**: Production pipelines must handle failures gracefully. These improvements move the pipeline from "it works" to "it works reliably."

---

## Phase 3: Advanced Quality

### Data Contracts
- [ ] Define formal data contracts (JSON Schema) for each dataset
- [ ] Implement contract enforcement at Bronze-to-Silver boundary
- [ ] Add contract versioning and backward compatibility checks
- [ ] Create contract violation alerting

### Quality Trending
- [ ] Store historical quality scores in DuckDB
- [ ] Build quality trend dashboard (score over time per dataset)
- [ ] Implement quality regression detection (alert when scores drop)
- [ ] Add data drift detection (schema changes, distribution shifts)

### Advanced Anomaly Detection
- [ ] Add time-series anomaly detection (seasonal patterns)
- [ ] Implement multi-column anomaly detection (correlation-based)
- [ ] Add configurable anomaly suppression (known outliers)
- [ ] Build anomaly investigation workflow

**Why this matters**: Data quality is what separates amateur pipelines from production ones. These features build trust in the data.

---

## Phase 4: Scalability

### Parallel Processing
- [ ] Add concurrent file processing (process multiple files simultaneously)
- [ ] Implement chunked processing for large files (> 100MB)
- [ ] Add async API calls with connection pooling
- [ ] Benchmark pipeline performance and add metrics

### Storage Optimization
- [ ] Implement partitioned Parquet storage (by date, by source)
- [ ] Add data retention policies (auto-archive old Bronze data)
- [ ] Implement incremental Silver updates (don't reprocess everything)
- [ ] Add Parquet file compaction (merge small files)

### Cloud Readiness
- [ ] Add S3-compatible storage backend (MinIO for local, S3 for cloud)
- [ ] Implement Snowflake connector as alternative Gold target
- [ ] Add Docker containerization with compose file
- [ ] Create CI/CD pipeline (GitHub Actions) for automated testing

**Why this matters**: Real pipelines process millions of rows. These changes prepare for scale without rewriting the architecture.

---

## Phase 5: Analytics and Visualization

### Streamlit Dashboard Enhancements
- [ ] Add workout progression charts (volume, weight, frequency)
- [ ] Build nutrition macro breakdown visualizations
- [ ] Create body composition trend analysis
- [ ] Add exercise frequency heatmaps
- [ ] Implement cross-dataset correlation analysis

### Analytical Views
- [ ] Build materialized views in DuckDB for common queries
- [ ] Add weekly training summary aggregation
- [ ] Create athlete performance scorecards
- [ ] Implement exercise recommendation engine (most neglected muscle groups)

### Export and Sharing
- [ ] Add PDF report generation for quality reports
- [ ] Create CSV/Excel export for Gold tables
- [ ] Build a simple REST API to serve warehouse data
- [ ] Add webhook notifications for pipeline events

---

## Phase 6: Supabase Integration

### Sync to Cloud
- [ ] Implement DuckDB -> Supabase sync (push Gold data to Postgres)
- [ ] Add bidirectional sync (Supabase changes -> local pipeline)
- [ ] Use Supabase Realtime for live workout logging
- [ ] Implement Supabase Edge Functions for API endpoints

### Frontend
- [ ] Build a simple web UI for file uploads (replace manual file drops)
- [ ] Create a workout logging form connected to the pipeline
- [ ] Add real-time quality score display
- [ ] Implement user authentication via Supabase Auth

---

## Contributing Ideas

If you're extending this project, here are good first issues:

1. **Add a new data source**: Weather API (correlate weather with outdoor workouts)
2. **New quality rule**: Check that workout dates are not in the future
3. **New enrichment**: Calculate training volume moving averages
4. **New dimension**: Build `dim_time_of_day` (morning/afternoon/evening workouts)
5. **New test**: Add property-based testing with Hypothesis

---

## Design Principles

These principles guide all development on this project:

1. **Teach, don't just code** -- Every module has LEARN comments explaining the "why"
2. **Config-driven** -- Change behavior via YAML, not Python code
3. **Fail safely** -- Bad data is quarantined, not lost
4. **Test everything** -- 80%+ coverage target
5. **Local first** -- Everything runs on your laptop, no cloud required
6. **Mirror production** -- Patterns transfer directly to Snowflake/Airflow/dbt
