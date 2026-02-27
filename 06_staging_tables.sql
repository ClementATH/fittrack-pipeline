-- ============================================================
-- FITTRACK PRO - ETL Pipeline Extension
-- File 6 of 7: STAGING & AUDIT TABLES
--
-- These tables support the ETL pipeline by providing:
--   1. Staging areas for raw data before transformation
--   2. Audit logging for tracking all data changes
--   3. Pipeline run metadata for monitoring
--
-- LEARN: Staging tables are temporary holding areas. Data lands here
-- first from external sources, gets validated and transformed, then
-- moves to the production tables. This pattern protects your production
-- data from corrupt or invalid records.
-- ============================================================


-- ============================================================
-- STAGING: Raw exercise data from Wger API
-- Data lands here before being validated and merged into exercises
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_exercises (
  stg_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id INTEGER,  -- Wger API exercise ID
  name TEXT,
  description TEXT,
  primary_muscle_id INTEGER,
  secondary_muscle_ids INTEGER[],
  equipment_ids INTEGER[],
  category_id INTEGER,
  -- ETL metadata
  batch_id UUID NOT NULL,
  source_name TEXT NOT NULL DEFAULT 'wger_api',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  processing_status TEXT DEFAULT 'pending'
    CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
  error_message TEXT,
  source_hash TEXT  -- MD5 hash for change detection
);

CREATE INDEX idx_stg_exercises_status ON stg_exercises(processing_status);
CREATE INDEX idx_stg_exercises_batch ON stg_exercises(batch_id);


-- ============================================================
-- STAGING: Raw nutrition data from USDA API
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_nutrition_foods (
  stg_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fdc_id INTEGER,  -- USDA FoodData Central ID
  food_name TEXT,
  data_type TEXT,
  calories DECIMAL(8,2),
  protein_g DECIMAL(8,2),
  carbs_g DECIMAL(8,2),
  fats_g DECIMAL(8,2),
  fiber_g DECIMAL(8,2),
  sugar_g DECIMAL(8,2),
  sodium_mg INTEGER,
  serving_size DECIMAL(8,2),
  serving_unit TEXT,
  -- ETL metadata
  batch_id UUID NOT NULL,
  source_name TEXT NOT NULL DEFAULT 'usda_api',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  processing_status TEXT DEFAULT 'pending'
    CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
  error_message TEXT,
  source_hash TEXT
);

CREATE INDEX idx_stg_nutrition_status ON stg_nutrition_foods(processing_status);


-- ============================================================
-- STAGING: File-based workout imports
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_workout_imports (
  stg_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_email TEXT,
  workout_date DATE,
  exercise_name TEXT,
  set_number INTEGER,
  weight_value DECIMAL(8,2),
  weight_unit TEXT DEFAULT 'kg',
  reps INTEGER,
  rpe DECIMAL(3,1),
  notes TEXT,
  source_file TEXT,
  -- ETL metadata
  batch_id UUID NOT NULL,
  source_name TEXT NOT NULL DEFAULT 'file_import',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  processing_status TEXT DEFAULT 'pending'
    CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
  error_message TEXT
);


-- ============================================================
-- AUDIT: Track all data changes across the pipeline
--
-- LEARN: Audit tables answer the question "what changed, when, and why?"
-- In healthcare (WellMed), audit trails are legally required by HIPAA.
-- Even outside healthcare, they're essential for debugging and compliance.
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- What happened
  event_type TEXT NOT NULL
    CHECK (event_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'SCD_CLOSE', 'SCD_INSERT')),
  target_table TEXT NOT NULL,
  target_record_id TEXT,
  -- When and where
  event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  pipeline_run_id UUID,
  batch_id UUID,
  source_name TEXT,
  -- Change details
  old_values JSONB,  -- Previous state (for updates)
  new_values JSONB,  -- New state
  change_reason TEXT,
  -- Who/what triggered it
  triggered_by TEXT DEFAULT 'etl_pipeline'
);

CREATE INDEX idx_audit_table ON etl_audit_log(target_table);
CREATE INDEX idx_audit_timestamp ON etl_audit_log(event_timestamp DESC);
CREATE INDEX idx_audit_run ON etl_audit_log(pipeline_run_id);


-- ============================================================
-- PIPELINE RUNS: Track every pipeline execution
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_run_log (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name TEXT NOT NULL,
  source_name TEXT,
  layer TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold', 'quality', 'full')),
  status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'partial')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  duration_seconds DECIMAL(10,2),
  rows_processed INTEGER DEFAULT 0,
  rows_failed INTEGER DEFAULT 0,
  rows_skipped INTEGER DEFAULT 0,
  error_message TEXT,
  -- Quality metrics for this run
  quality_score DECIMAL(5,2),
  quality_grade TEXT,
  -- Configuration snapshot (what settings were used)
  config_snapshot JSONB,
  -- Machine-readable metadata
  metadata JSONB
);

CREATE INDEX idx_runs_status ON pipeline_run_log(status);
CREATE INDEX idx_runs_started ON pipeline_run_log(started_at DESC);
CREATE INDEX idx_runs_source ON pipeline_run_log(source_name);


-- ============================================================
-- DATA QUALITY SCORES: Historical quality tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS data_quality_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  table_name TEXT NOT NULL,
  run_id UUID REFERENCES pipeline_run_log(run_id),
  scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  overall_score DECIMAL(5,2) NOT NULL,
  completeness_score DECIMAL(5,2),
  accuracy_score DECIMAL(5,2),
  consistency_score DECIMAL(5,2),
  timeliness_score DECIMAL(5,2),
  row_count INTEGER,
  failed_checks INTEGER DEFAULT 0,
  total_checks INTEGER DEFAULT 0,
  details JSONB
);

CREATE INDEX idx_quality_table ON data_quality_history(table_name);
CREATE INDEX idx_quality_scored ON data_quality_history(scored_at DESC);


-- ============================================================
-- SUCCESS MESSAGE
-- ============================================================

DO $$ BEGIN RAISE NOTICE '
Staging and audit tables created:
  - stg_exercises (Wger API staging)
  - stg_nutrition_foods (USDA API staging)
  - stg_workout_imports (file import staging)
  - etl_audit_log (change tracking)
  - pipeline_run_log (run history)
  - data_quality_history (quality scores)

Run 07_etl_procedures.sql next.
'; END $$;
