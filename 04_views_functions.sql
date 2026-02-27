-- ============================================================
-- FITTRACK PRO - Supabase Fitness Database
-- File 4 of 5: VIEWS, FUNCTIONS, RPCs
-- 
-- Paste this FOURTH into Supabase SQL Editor
-- Teaches: Views, Functions, RPCs (callable from client SDK)
-- ============================================================


-- ============================================================
-- VIEW: Weekly Training Summary
-- Aggregates workout data by week
-- In Supabase, views appear in Table Editor and are queryable
-- via the client SDK just like tables
-- ============================================================

CREATE OR REPLACE VIEW weekly_training_summary AS
SELECT
  w.athlete_id,
  DATE_TRUNC('week', w.workout_date)::date AS week_start,
  COUNT(*) AS workouts_completed,
  ROUND(AVG(w.duration_minutes)::numeric, 0) AS avg_duration_min,
  ROUND(SUM(w.total_volume_kg)::numeric, 0) AS total_weekly_volume_kg,
  ROUND(AVG(w.total_volume_kg)::numeric, 0) AS avg_volume_per_session,
  ROUND(AVG(w.energy_level)::numeric, 1) AS avg_energy,
  ROUND(AVG(w.pump_rating)::numeric, 1) AS avg_pump,
  ROUND(AVG(w.sleep_hours_prior)::numeric, 1) AS avg_sleep_hours,
  COUNT(*) FILTER (WHERE w.status = 'skipped') AS sessions_skipped
FROM workouts w
GROUP BY w.athlete_id, DATE_TRUNC('week', w.workout_date)
ORDER BY week_start DESC;


-- ============================================================
-- VIEW: Exercise Performance Over Time
-- Track progressive overload per exercise
-- ============================================================

CREATE OR REPLACE VIEW exercise_progression AS
SELECT
  w.athlete_id,
  e.name AS exercise_name,
  e.primary_muscle,
  w.workout_date,
  COUNT(ws.id) AS total_sets,
  COUNT(ws.id) FILTER (WHERE ws.set_type = 'working') AS working_sets,
  ROUND(MAX(ws.weight_kg)::numeric, 1) AS max_weight_kg,
  ROUND(AVG(ws.weight_kg) FILTER (WHERE ws.set_type = 'working')::numeric, 1) AS avg_working_weight_kg,
  ROUND(AVG(ws.reps) FILTER (WHERE ws.set_type = 'working')::numeric, 1) AS avg_reps,
  ROUND(AVG(ws.rpe) FILTER (WHERE ws.set_type = 'working')::numeric, 1) AS avg_rpe,
  ROUND(SUM(ws.weight_kg * ws.reps) FILTER (WHERE ws.set_type = 'working')::numeric, 0) AS exercise_volume_kg
FROM workout_sets ws
JOIN workouts w ON ws.workout_id = w.id
JOIN exercises e ON ws.exercise_id = e.id
GROUP BY w.athlete_id, e.name, e.primary_muscle, w.workout_date
ORDER BY e.name, w.workout_date;


-- ============================================================
-- VIEW: Daily Macro Totals
-- Aggregate nutrition per day (useful for dashboards)
-- ============================================================

CREATE OR REPLACE VIEW daily_nutrition_totals AS
SELECT
  athlete_id,
  log_date,
  SUM(calories) AS total_calories,
  ROUND(SUM(protein_g)::numeric, 1) AS total_protein_g,
  ROUND(SUM(carbs_g)::numeric, 1) AS total_carbs_g,
  ROUND(SUM(fats_g)::numeric, 1) AS total_fats_g,
  ROUND(SUM(fiber_g)::numeric, 1) AS total_fiber_g,
  SUM(water_ml) AS total_water_ml,
  COUNT(*) AS meals_logged,
  -- Macro ratios
  ROUND((SUM(protein_g) * 4 / NULLIF(SUM(calories), 0) * 100)::numeric, 1) AS protein_pct,
  ROUND((SUM(carbs_g) * 4 / NULLIF(SUM(calories), 0) * 100)::numeric, 1) AS carbs_pct,
  ROUND((SUM(fats_g) * 9 / NULLIF(SUM(calories), 0) * 100)::numeric, 1) AS fats_pct
FROM nutrition_logs
GROUP BY athlete_id, log_date
ORDER BY log_date DESC;


-- ============================================================
-- VIEW: Body Composition Trend
-- Weekly body composition snapshots with calculated fields
-- ============================================================

CREATE OR REPLACE VIEW body_composition_weekly AS
SELECT
  athlete_id,
  measured_at,
  weight_kg,
  body_fat_pct,
  lean_mass_kg,
  ROUND((weight_kg * body_fat_pct / 100)::numeric, 1) AS fat_mass_kg,
  waist_cm,
  chest_cm,
  left_arm_cm,
  right_arm_cm,
  -- Arm symmetry check (important for bodybuilding)
  ROUND(ABS(COALESCE(right_arm_cm, 0) - COALESCE(left_arm_cm, 0))::numeric, 1) AS arm_difference_cm,
  resting_heart_rate,
  recovery_score,
  steps
FROM body_metrics
WHERE waist_cm IS NOT NULL -- only weekly measurement days
ORDER BY measured_at DESC;


-- ============================================================
-- VIEW: Muscle Group Volume Distribution
-- How much volume per muscle group per week
-- ============================================================

CREATE OR REPLACE VIEW muscle_volume_distribution AS
SELECT
  w.athlete_id,
  DATE_TRUNC('week', w.workout_date)::date AS week_start,
  e.primary_muscle,
  COUNT(ws.id) FILTER (WHERE ws.set_type = 'working') AS working_sets,
  ROUND(SUM(ws.weight_kg * ws.reps) FILTER (WHERE ws.set_type = 'working')::numeric, 0) AS total_volume_kg,
  ROUND(AVG(ws.rpe) FILTER (WHERE ws.set_type = 'working')::numeric, 1) AS avg_rpe
FROM workout_sets ws
JOIN workouts w ON ws.workout_id = w.id
JOIN exercises e ON ws.exercise_id = e.id
WHERE w.status = 'completed'
GROUP BY w.athlete_id, DATE_TRUNC('week', w.workout_date), e.primary_muscle
ORDER BY week_start DESC, total_volume_kg DESC;


-- ============================================================
-- FUNCTION: Calculate Estimated 1RM
-- Epley Formula: 1RM = weight × (1 + reps/30)
-- Callable via Supabase RPC: supabase.rpc('calculate_e1rm', {...})
-- ============================================================

CREATE OR REPLACE FUNCTION calculate_e1rm(
  p_weight DECIMAL,
  p_reps INTEGER
)
RETURNS DECIMAL
LANGUAGE plpgsql
IMMUTABLE -- Pure function, same inputs = same output (enables caching)
AS $$
BEGIN
  IF p_reps = 1 THEN
    RETURN p_weight;
  END IF;
  RETURN ROUND((p_weight * (1 + p_reps::decimal / 30))::numeric, 1);
END;
$$;


-- ============================================================
-- FUNCTION: Get Workout Detail (full workout with all sets)
-- Returns a complete workout session as JSON
-- Callable via: supabase.rpc('get_workout_detail', { workout_id: '...' })
-- ============================================================

CREATE OR REPLACE FUNCTION get_workout_detail(p_workout_id UUID)
RETURNS JSON
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_result JSON;
BEGIN
  SELECT json_build_object(
    'workout', json_build_object(
      'id', w.id,
      'date', w.workout_date,
      'day_name', w.day_name,
      'status', w.status,
      'duration_minutes', w.duration_minutes,
      'total_volume_kg', w.total_volume_kg,
      'energy_level', w.energy_level,
      'pump_rating', w.pump_rating,
      'notes', w.notes
    ),
    'exercises', (
      SELECT json_agg(
        json_build_object(
          'exercise_name', e.name,
          'muscle_group', e.primary_muscle,
          'equipment', e.equipment,
          'sets', (
            SELECT json_agg(
              json_build_object(
                'set_number', ws2.set_number,
                'set_type', ws2.set_type,
                'weight_kg', ws2.weight_kg,
                'reps', ws2.reps,
                'rpe', ws2.rpe,
                'rest_seconds', ws2.rest_seconds
              ) ORDER BY ws2.set_number
            )
            FROM workout_sets ws2
            WHERE ws2.workout_id = w.id AND ws2.exercise_id = e.id
          )
        )
      )
      FROM (
        SELECT DISTINCT ws.exercise_id, MIN(ws.set_number) as first_set
        FROM workout_sets ws
        WHERE ws.workout_id = w.id
        GROUP BY ws.exercise_id
        ORDER BY MIN(ws.set_number)
      ) ordered_ex
      JOIN exercises e ON e.id = ordered_ex.exercise_id
    ),
    'tags', (
      SELECT json_agg(t.name)
      FROM workout_tag_assignments wta
      JOIN workout_tags t ON t.id = wta.tag_id
      WHERE wta.workout_id = w.id
    )
  ) INTO v_result
  FROM workouts w
  WHERE w.id = p_workout_id;
  
  RETURN v_result;
END;
$$;


-- ============================================================
-- FUNCTION: Get Training Stats for Dashboard
-- Returns overview stats for a given time period
-- ============================================================

CREATE OR REPLACE FUNCTION get_training_stats(
  p_athlete_id UUID,
  p_start_date DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
  p_end_date DATE DEFAULT CURRENT_DATE
)
RETURNS JSON
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_result JSON;
BEGIN
  SELECT json_build_object(
    'period', json_build_object(
      'start_date', p_start_date,
      'end_date', p_end_date,
      'days', p_end_date - p_start_date
    ),
    'workouts', json_build_object(
      'total_sessions', COUNT(*),
      'completed', COUNT(*) FILTER (WHERE status = 'completed'),
      'skipped', COUNT(*) FILTER (WHERE status = 'skipped'),
      'completion_rate', ROUND(
        COUNT(*) FILTER (WHERE status = 'completed')::decimal / 
        NULLIF(COUNT(*), 0) * 100, 1
      ),
      'total_hours', ROUND(SUM(duration_minutes)::decimal / 60, 1),
      'avg_duration_min', ROUND(AVG(duration_minutes)::numeric, 0),
      'total_volume_kg', ROUND(SUM(total_volume_kg)::numeric, 0),
      'avg_energy', ROUND(AVG(energy_level)::numeric, 1)
    ),
    'body', (
      SELECT json_build_object(
        'start_weight', (SELECT weight_kg FROM body_metrics WHERE athlete_id = p_athlete_id AND measured_at >= p_start_date ORDER BY measured_at ASC LIMIT 1),
        'current_weight', (SELECT weight_kg FROM body_metrics WHERE athlete_id = p_athlete_id AND measured_at <= p_end_date ORDER BY measured_at DESC LIMIT 1),
        'avg_body_fat', ROUND(AVG(body_fat_pct)::numeric, 1),
        'avg_recovery', ROUND(AVG(recovery_score)::numeric, 0)
      )
      FROM body_metrics
      WHERE athlete_id = p_athlete_id
        AND measured_at BETWEEN p_start_date AND p_end_date
    ),
    'nutrition_avg', (
      SELECT json_build_object(
        'avg_calories', ROUND(AVG(total_calories)::numeric, 0),
        'avg_protein', ROUND(AVG(total_protein_g)::numeric, 0),
        'avg_carbs', ROUND(AVG(total_carbs_g)::numeric, 0),
        'avg_fats', ROUND(AVG(total_fats_g)::numeric, 0)
      )
      FROM daily_nutrition_totals
      WHERE athlete_id = p_athlete_id
        AND log_date BETWEEN p_start_date AND p_end_date
    ),
    'prs_hit', (
      SELECT COUNT(*)
      FROM personal_records
      WHERE athlete_id = p_athlete_id
        AND achieved_date BETWEEN p_start_date AND p_end_date
    )
  ) INTO v_result
  FROM workouts
  WHERE athlete_id = p_athlete_id
    AND workout_date BETWEEN p_start_date AND p_end_date;
  
  RETURN v_result;
END;
$$;


-- ============================================================
-- FUNCTION: Get Exercise E1RM History
-- Shows estimated 1RM progression for any exercise
-- ============================================================

CREATE OR REPLACE FUNCTION get_e1rm_history(
  p_athlete_id UUID,
  p_exercise_slug TEXT
)
RETURNS TABLE(
  workout_date DATE,
  best_weight_kg DECIMAL,
  best_reps INTEGER,
  estimated_1rm DECIMAL
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT
    w.workout_date,
    ws.weight_kg AS best_weight_kg,
    ws.reps AS best_reps,
    calculate_e1rm(ws.weight_kg, ws.reps) AS estimated_1rm
  FROM workout_sets ws
  JOIN workouts w ON ws.workout_id = w.id
  JOIN exercises e ON ws.exercise_id = e.id
  WHERE w.athlete_id = p_athlete_id
    AND e.slug = p_exercise_slug
    AND ws.set_type = 'working'
    AND ws.weight_kg = (
      -- Get the heaviest set per workout for this exercise
      SELECT MAX(ws2.weight_kg)
      FROM workout_sets ws2
      WHERE ws2.workout_id = w.id
        AND ws2.exercise_id = e.id
        AND ws2.set_type = 'working'
    )
  ORDER BY w.workout_date;
END;
$$;


-- ============================================================
-- FUNCTION: Search Exercises (Full-text search)
-- Demonstrates Supabase full-text search capabilities
-- ============================================================

-- First, add a tsvector column for full-text search
ALTER TABLE exercises ADD COLUMN IF NOT EXISTS search_vector tsvector;

UPDATE exercises SET search_vector = 
  to_tsvector('english', 
    COALESCE(name, '') || ' ' || 
    COALESCE(primary_muscle::text, '') || ' ' ||
    COALESCE(exercise_type::text, '') || ' ' ||
    COALESCE(equipment::text, '') || ' ' ||
    COALESCE(instructions, '')
  );

CREATE INDEX idx_exercises_search ON exercises USING gin(search_vector);

-- Create trigger to auto-update search vector
CREATE OR REPLACE FUNCTION update_exercise_search_vector()
RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector := to_tsvector('english',
    COALESCE(NEW.name, '') || ' ' ||
    COALESCE(NEW.primary_muscle::text, '') || ' ' ||
    COALESCE(NEW.exercise_type::text, '') || ' ' ||
    COALESCE(NEW.equipment::text, '') || ' ' ||
    COALESCE(NEW.instructions, '')
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER exercise_search_update
  BEFORE INSERT OR UPDATE ON exercises
  FOR EACH ROW EXECUTE FUNCTION update_exercise_search_vector();

-- Search function
CREATE OR REPLACE FUNCTION search_exercises(search_query TEXT)
RETURNS SETOF exercises
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT *
  FROM exercises
  WHERE search_vector @@ plainto_tsquery('english', search_query)
  ORDER BY ts_rank(search_vector, plainto_tsquery('english', search_query)) DESC;
END;
$$;


-- ============================================================
-- FUNCTION: Log a Complete Set (with auto-PR detection)
-- Shows how to use transactions and conditional logic
-- ============================================================

CREATE OR REPLACE FUNCTION log_set(
  p_workout_id UUID,
  p_exercise_slug TEXT,
  p_set_number INTEGER,
  p_set_type set_type,
  p_weight_kg DECIMAL,
  p_reps INTEGER,
  p_rpe DECIMAL DEFAULT NULL
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
  v_exercise_id UUID;
  v_athlete_id UUID;
  v_set_id UUID;
  v_e1rm DECIMAL;
  v_current_pr DECIMAL;
  v_is_pr BOOLEAN := FALSE;
BEGIN
  -- Get exercise ID
  SELECT id INTO v_exercise_id FROM exercises WHERE slug = p_exercise_slug;
  IF v_exercise_id IS NULL THEN
    RAISE EXCEPTION 'Exercise not found: %', p_exercise_slug;
  END IF;
  
  -- Get athlete ID from workout
  SELECT athlete_id INTO v_athlete_id FROM workouts WHERE id = p_workout_id;
  
  -- Calculate e1RM
  v_e1rm := calculate_e1rm(p_weight_kg, p_reps);
  
  -- Check if this is a PR
  SELECT value INTO v_current_pr
  FROM personal_records
  WHERE athlete_id = v_athlete_id
    AND exercise_id = v_exercise_id
    AND record_type = '1rm';
  
  IF v_current_pr IS NULL OR v_e1rm > v_current_pr THEN
    v_is_pr := TRUE;
    
    -- Upsert the PR record
    INSERT INTO personal_records (athlete_id, exercise_id, record_type, value, achieved_date, previous_value, workout_id)
    VALUES (v_athlete_id, v_exercise_id, '1rm', v_e1rm, CURRENT_DATE, v_current_pr, p_workout_id)
    ON CONFLICT (athlete_id, exercise_id, record_type)
    DO UPDATE SET
      value = v_e1rm,
      previous_value = v_current_pr,
      achieved_date = CURRENT_DATE,
      workout_id = p_workout_id;
  END IF;
  
  -- Insert the set
  INSERT INTO workout_sets (
    workout_id, exercise_id, set_number, set_type,
    weight_kg, reps, rpe, is_pr
  ) VALUES (
    p_workout_id, v_exercise_id, p_set_number, p_set_type,
    p_weight_kg, p_reps, p_rpe, v_is_pr
  )
  RETURNING id INTO v_set_id;
  
  RETURN json_build_object(
    'set_id', v_set_id,
    'estimated_1rm', v_e1rm,
    'is_pr', v_is_pr,
    'previous_pr', v_current_pr
  );
END;
$$;


DO $$ BEGIN RAISE NOTICE '✅ Views and functions created! Run 05_rls_policies.sql next.'; END $$;
