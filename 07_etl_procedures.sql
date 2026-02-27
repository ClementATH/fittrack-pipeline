-- ============================================================
-- FITTRACK PRO - ETL Pipeline Extension
-- File 7 of 7: ETL PROCEDURES & TRANSFORMATION FUNCTIONS
--
-- SQL-side transformation logic that complements the Python pipeline.
-- These procedures handle the Silver -> Gold data movement within
-- the database itself.
--
-- LEARN: Having transformation logic in SQL procedures means:
--   1. Data stays in the database (no round-trip to Python)
--   2. Transformations are transactional (all-or-nothing)
--   3. DBAs and SQL-savvy team members can modify them
--   4. They run closer to the data (faster for large datasets)
-- At WellMed with Snowflake, stored procedures serve the same role.
-- ============================================================


-- ============================================================
-- PROCEDURE: Process staged exercises into the exercises table
--
-- LEARN: This is a classic "MERGE" / "UPSERT" pattern.
-- If the exercise already exists (by slug), update it.
-- If it's new, insert it. This is idempotent — running it
-- multiple times on the same data produces the same result.
-- ============================================================

CREATE OR REPLACE FUNCTION process_staged_exercises(p_batch_id UUID)
RETURNS TABLE (
  inserted INTEGER,
  updated INTEGER,
  skipped INTEGER,
  failed INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_inserted INTEGER := 0;
  v_updated INTEGER := 0;
  v_skipped INTEGER := 0;
  v_failed INTEGER := 0;
  v_record RECORD;
  v_slug TEXT;
  v_primary_muscle muscle_group;
  v_equipment equipment_type;
BEGIN
  -- Mark batch as processing
  UPDATE stg_exercises
  SET processing_status = 'processing'
  WHERE batch_id = p_batch_id AND processing_status = 'pending';

  -- Process each staged record
  FOR v_record IN
    SELECT * FROM stg_exercises
    WHERE batch_id = p_batch_id AND processing_status = 'processing'
  LOOP
    BEGIN
      -- Generate slug from name
      v_slug := lower(regexp_replace(v_record.name, '[^a-zA-Z0-9\s]', '', 'g'));
      v_slug := regexp_replace(v_slug, '\s+', '-', 'g');
      v_slug := trim(BOTH '-' FROM v_slug);

      -- Check if exercise already exists
      IF EXISTS (SELECT 1 FROM exercises WHERE slug = v_slug) THEN
        -- Update existing exercise
        UPDATE exercises
        SET
          name = COALESCE(v_record.name, name),
          instructions = COALESCE(v_record.description, instructions),
          created_at = NOW()
        WHERE slug = v_slug;

        v_updated := v_updated + 1;

        -- Audit log
        INSERT INTO etl_audit_log (event_type, target_table, target_record_id, batch_id, source_name, change_reason)
        VALUES ('UPDATE', 'exercises', v_slug, p_batch_id, 'wger_api', 'Staged exercise update');
      ELSE
        -- Insert new exercise (with safe defaults for enum types)
        INSERT INTO exercises (name, slug, primary_muscle, exercise_type, equipment, difficulty, instructions, is_custom)
        VALUES (
          v_record.name,
          v_slug,
          'full_body',     -- Default; would be mapped from muscle_id in Python
          'compound',       -- Default
          'none',           -- Default
          'intermediate',   -- Default
          v_record.description,
          FALSE
        );

        v_inserted := v_inserted + 1;

        INSERT INTO etl_audit_log (event_type, target_table, target_record_id, batch_id, source_name, change_reason)
        VALUES ('INSERT', 'exercises', v_slug, p_batch_id, 'wger_api', 'New exercise from staging');
      END IF;

      -- Mark as completed
      UPDATE stg_exercises SET processing_status = 'completed', processed_at = NOW()
      WHERE stg_id = v_record.stg_id;

    EXCEPTION WHEN OTHERS THEN
      -- Mark as failed with error message
      UPDATE stg_exercises
      SET processing_status = 'failed', error_message = SQLERRM, processed_at = NOW()
      WHERE stg_id = v_record.stg_id;

      v_failed := v_failed + 1;
    END;
  END LOOP;

  RETURN QUERY SELECT v_inserted, v_updated, v_skipped, v_failed;
END;
$$;


-- ============================================================
-- PROCEDURE: Calculate training volume aggregates
--
-- Aggregates workout set data into weekly and monthly summaries.
-- These feed the Gold layer analytics tables.
-- ============================================================

CREATE OR REPLACE FUNCTION calculate_weekly_volume(
  p_athlete_id UUID,
  p_week_start DATE DEFAULT NULL
)
RETURNS TABLE (
  week_start DATE,
  total_workouts INTEGER,
  total_sets INTEGER,
  total_volume_kg DECIMAL(12,2),
  total_reps INTEGER,
  avg_rpe DECIMAL(3,1),
  muscles_trained TEXT[],
  training_days INTEGER
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT
    DATE_TRUNC('week', w.workout_date)::DATE AS week_start,
    COUNT(DISTINCT w.id)::INTEGER AS total_workouts,
    COUNT(ws.id)::INTEGER AS total_sets,
    ROUND(SUM(COALESCE(ws.weight_kg, 0) * COALESCE(ws.reps, 0))::DECIMAL, 2) AS total_volume_kg,
    SUM(COALESCE(ws.reps, 0))::INTEGER AS total_reps,
    ROUND(AVG(ws.rpe)::DECIMAL, 1) AS avg_rpe,
    ARRAY_AGG(DISTINCT e.primary_muscle::TEXT) AS muscles_trained,
    COUNT(DISTINCT w.workout_date)::INTEGER AS training_days
  FROM workouts w
  JOIN workout_sets ws ON ws.workout_id = w.id
  JOIN exercises e ON ws.exercise_id = e.id
  WHERE w.athlete_id = p_athlete_id
    AND w.status = 'completed'
    AND ws.set_type != 'warmup'
    AND (p_week_start IS NULL OR DATE_TRUNC('week', w.workout_date)::DATE = p_week_start)
  GROUP BY DATE_TRUNC('week', w.workout_date)
  ORDER BY week_start DESC;
END;
$$;


-- ============================================================
-- PROCEDURE: Detect personal records automatically
--
-- LEARN: This is a practical example of a database procedure that
-- adds business intelligence. When new workout data arrives, this
-- checks if any records were broken and updates the PRs table.
-- ============================================================

CREATE OR REPLACE FUNCTION detect_new_prs(
  p_athlete_id UUID,
  p_workout_id UUID
)
RETURNS TABLE (
  exercise_name TEXT,
  record_type TEXT,
  new_value DECIMAL(8,2),
  previous_value DECIMAL(8,2),
  improvement_pct DECIMAL(5,2)
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_record RECORD;
  v_estimated_1rm DECIMAL(8,2);
  v_current_pr DECIMAL(8,2);
BEGIN
  -- Check each working set for potential PRs
  FOR v_record IN
    SELECT
      ws.weight_kg,
      ws.reps,
      e.id AS exercise_id,
      e.name AS exercise_name,
      e.slug
    FROM workout_sets ws
    JOIN exercises e ON ws.exercise_id = e.id
    WHERE ws.workout_id = p_workout_id
      AND ws.set_type = 'working'
      AND ws.weight_kg IS NOT NULL
      AND ws.reps IS NOT NULL
      AND ws.reps > 0
  LOOP
    -- Calculate estimated 1RM using Epley formula
    -- LEARN: The Epley formula estimates your one-rep max from
    -- a set with multiple reps: E1RM = weight * (1 + reps/30)
    v_estimated_1rm := ROUND(
      v_record.weight_kg * (1 + v_record.reps::DECIMAL / 30), 1
    );

    -- Check if this beats the current 1RM PR
    SELECT value INTO v_current_pr
    FROM personal_records
    WHERE athlete_id = p_athlete_id
      AND exercise_id = v_record.exercise_id
      AND record_type = '1rm';

    IF v_current_pr IS NULL OR v_estimated_1rm > v_current_pr THEN
      -- UPSERT the PR
      INSERT INTO personal_records (athlete_id, exercise_id, record_type, value, achieved_date, workout_id, previous_value)
      VALUES (p_athlete_id, v_record.exercise_id, '1rm', v_estimated_1rm, CURRENT_DATE, p_workout_id, v_current_pr)
      ON CONFLICT (athlete_id, exercise_id, record_type)
      DO UPDATE SET
        value = EXCLUDED.value,
        previous_value = personal_records.value,
        achieved_date = EXCLUDED.achieved_date,
        workout_id = EXCLUDED.workout_id;

      RETURN QUERY SELECT
        v_record.exercise_name,
        '1rm'::TEXT,
        v_estimated_1rm,
        v_current_pr,
        CASE WHEN v_current_pr > 0
          THEN ROUND((v_estimated_1rm - v_current_pr) / v_current_pr * 100, 2)
          ELSE 100.0
        END;
    END IF;
  END LOOP;
END;
$$;


-- ============================================================
-- PROCEDURE: Data quality check summary
--
-- Returns a quick health check of all main tables.
-- ============================================================

CREATE OR REPLACE FUNCTION pipeline_health_summary()
RETURNS TABLE (
  table_name TEXT,
  row_count BIGINT,
  null_percentage DECIMAL(5,2),
  last_updated TIMESTAMPTZ,
  status TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY

  -- Athletes
  SELECT 'athletes'::TEXT, COUNT(*)::BIGINT, 0.0::DECIMAL(5,2),
    MAX(updated_at), 'healthy'::TEXT
  FROM athletes

  UNION ALL

  -- Exercises
  SELECT 'exercises', COUNT(*), 0.0,
    MAX(created_at), 'healthy'
  FROM exercises

  UNION ALL

  -- Workouts
  SELECT 'workouts', COUNT(*), 0.0,
    MAX(created_at),
    CASE WHEN MAX(workout_date) < CURRENT_DATE - INTERVAL '7 days' THEN 'stale' ELSE 'healthy' END
  FROM workouts

  UNION ALL

  -- Workout Sets
  SELECT 'workout_sets', COUNT(*), 0.0,
    MAX(created_at), 'healthy'
  FROM workout_sets

  UNION ALL

  -- Body Metrics
  SELECT 'body_metrics', COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN weight_kg IS NULL THEN 1 ELSE 0 END)::DECIMAL / GREATEST(COUNT(*), 1), 2),
    MAX(created_at),
    CASE WHEN MAX(measured_at) < CURRENT_DATE - INTERVAL '3 days' THEN 'stale' ELSE 'healthy' END
  FROM body_metrics

  UNION ALL

  -- Nutrition Logs
  SELECT 'nutrition_logs', COUNT(*), 0.0,
    MAX(created_at),
    CASE WHEN MAX(log_date) < CURRENT_DATE - INTERVAL '2 days' THEN 'stale' ELSE 'healthy' END
  FROM nutrition_logs

  ORDER BY table_name;
END;
$$;


-- ============================================================
-- SUCCESS MESSAGE
-- ============================================================

DO $$ BEGIN RAISE NOTICE '
ETL procedures created:
  - process_staged_exercises()  -- Merge staged data into exercises
  - calculate_weekly_volume()   -- Training volume aggregates
  - detect_new_prs()            -- Auto-detect personal records
  - pipeline_health_summary()   -- Quick data health check

Usage:
  SELECT * FROM process_staged_exercises(''your-batch-uuid'');
  SELECT * FROM calculate_weekly_volume(''a1b2c3d4-e5f6-7890-abcd-ef1234567890'');
  SELECT * FROM pipeline_health_summary();

DATABASE SETUP FULLY COMPLETE!
'; END $$;
