-- ============================================================
-- FITTRACK PRO - Supabase Fitness Database
-- File 1 of 5: SCHEMA (Tables, Types, Indexes, Constraints)
-- 
-- Paste this FIRST into Supabase SQL Editor
-- This creates all tables and relationships
-- ============================================================

-- ============================================================
-- CUSTOM ENUM TYPES
-- Supabase/Postgres lets you define custom types
-- These enforce data integrity at the database level
-- ============================================================

CREATE TYPE muscle_group AS ENUM (
  'chest', 'back', 'shoulders', 'biceps', 'triceps',
  'forearms', 'quads', 'hamstrings', 'glutes', 'calves',
  'abs', 'obliques', 'traps', 'lats', 'hip_flexors',
  'adductors', 'abductors', 'neck', 'full_body'
);

CREATE TYPE exercise_type AS ENUM (
  'compound', 'isolation', 'bodyweight', 'plyometric',
  'isometric', 'cardio', 'flexibility', 'olympic_lift'
);

CREATE TYPE equipment_type AS ENUM (
  'barbell', 'dumbbell', 'cable', 'machine', 'kettlebell',
  'bodyweight', 'resistance_band', 'smith_machine', 'ez_bar',
  'trap_bar', 'pull_up_bar', 'dip_station', 'suspension_trainer',
  'medicine_ball', 'battle_rope', 'sled', 'none'
);

CREATE TYPE difficulty_level AS ENUM (
  'beginner', 'intermediate', 'advanced', 'elite'
);

CREATE TYPE program_goal AS ENUM (
  'hypertrophy', 'strength', 'power', 'endurance',
  'fat_loss', 'recomp', 'athletic_performance', 'general_fitness'
);

CREATE TYPE split_type AS ENUM (
  'push_pull_legs', 'upper_lower', 'full_body', 'bro_split',
  'arnold_split', 'phat', 'phul', 'custom'
);

CREATE TYPE set_type AS ENUM (
  'working', 'warmup', 'dropset', 'superset', 'giant_set',
  'rest_pause', 'myo_rep', 'cluster', 'amrap', 'tempo',
  'paused', 'eccentric', 'isometric_hold'
);

CREATE TYPE workout_status AS ENUM (
  'completed', 'skipped', 'partial', 'planned'
);

CREATE TYPE meal_type AS ENUM (
  'breakfast', 'lunch', 'dinner', 'pre_workout',
  'post_workout', 'snack', 'supplement'
);

CREATE TYPE cardio_type AS ENUM (
  'liss', 'hiit', 'miss', 'sprints', 'steady_state'
);


-- ============================================================
-- TABLE: athletes
-- Core user/athlete profile
-- In production, this would link to Supabase Auth (auth.users)
-- ============================================================

CREATE TABLE athletes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- In production: auth_user_id UUID REFERENCES auth.users(id),
  email TEXT UNIQUE NOT NULL,
  username TEXT UNIQUE NOT NULL,
  full_name TEXT NOT NULL,
  date_of_birth DATE,
  gender TEXT CHECK (gender IN ('male', 'female', 'other')),
  height_cm DECIMAL(5,1),
  target_weight_kg DECIMAL(5,1),
  activity_level TEXT CHECK (activity_level IN (
    'sedentary', 'lightly_active', 'moderately_active', 
    'very_active', 'extremely_active'
  )),
  training_experience_years INTEGER DEFAULT 0,
  bio TEXT,
  instagram_handle TEXT,
  profile_image_url TEXT,
  timezone TEXT DEFAULT 'America/Chicago',
  unit_preference TEXT DEFAULT 'imperial' CHECK (unit_preference IN ('metric', 'imperial')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE: exercises
-- Master exercise library
-- ============================================================

CREATE TABLE exercises (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL, -- URL-friendly name
  primary_muscle muscle_group NOT NULL,
  secondary_muscles muscle_group[] DEFAULT '{}', -- Postgres ARRAY type
  exercise_type exercise_type NOT NULL,
  equipment equipment_type NOT NULL,
  difficulty difficulty_level NOT NULL DEFAULT 'intermediate',
  instructions TEXT,
  tips TEXT[], -- Array of coaching cues
  video_url TEXT,
  is_unilateral BOOLEAN DEFAULT FALSE, -- single arm/leg
  is_custom BOOLEAN DEFAULT FALSE,
  created_by UUID REFERENCES athletes(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast lookups by muscle group
CREATE INDEX idx_exercises_primary_muscle ON exercises(primary_muscle);
CREATE INDEX idx_exercises_type ON exercises(exercise_type);
CREATE INDEX idx_exercises_equipment ON exercises(equipment);


-- ============================================================
-- TABLE: workout_programs
-- Training programs / mesocycles
-- ============================================================

CREATE TABLE workout_programs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT,
  goal program_goal NOT NULL,
  split split_type NOT NULL,
  duration_weeks INTEGER NOT NULL CHECK (duration_weeks BETWEEN 1 AND 52),
  days_per_week INTEGER NOT NULL CHECK (days_per_week BETWEEN 1 AND 7),
  is_active BOOLEAN DEFAULT TRUE,
  start_date DATE NOT NULL,
  end_date DATE,
  deload_week INTEGER, -- which week is the deload
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_programs_athlete ON workout_programs(athlete_id);
CREATE INDEX idx_programs_active ON workout_programs(is_active) WHERE is_active = TRUE;


-- ============================================================
-- TABLE: workouts
-- Individual training sessions
-- ============================================================

CREATE TABLE workouts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  program_id UUID REFERENCES workout_programs(id) ON DELETE SET NULL,
  workout_date DATE NOT NULL,
  day_name TEXT, -- e.g., 'Push Day', 'Upper A', 'Legs'
  status workout_status NOT NULL DEFAULT 'completed',
  start_time TIMESTAMPTZ,
  end_time TIMESTAMPTZ,
  duration_minutes INTEGER GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (end_time - start_time)) / 60
  ) STORED, -- Computed column!
  total_volume_kg DECIMAL(10,2), -- sum of all weight * reps
  total_sets INTEGER,
  energy_level INTEGER CHECK (energy_level BETWEEN 1 AND 10),
  pump_rating INTEGER CHECK (pump_rating BETWEEN 1 AND 10),
  soreness_rating INTEGER CHECK (soreness_rating BETWEEN 1 AND 10),
  sleep_hours_prior DECIMAL(3,1),
  notes TEXT,
  location TEXT DEFAULT 'gym',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_workouts_athlete_date ON workouts(athlete_id, workout_date DESC);
CREATE INDEX idx_workouts_program ON workouts(program_id);
CREATE INDEX idx_workouts_status ON workouts(status);


-- ============================================================
-- TABLE: workout_sets
-- Individual sets within a workout (the core tracking data)
-- ============================================================

CREATE TABLE workout_sets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workout_id UUID NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
  exercise_id UUID NOT NULL REFERENCES exercises(id) ON DELETE RESTRICT,
  set_number INTEGER NOT NULL CHECK (set_number > 0),
  set_type set_type NOT NULL DEFAULT 'working',
  weight_kg DECIMAL(6,2),
  reps INTEGER CHECK (reps >= 0),
  rpe DECIMAL(3,1) CHECK (rpe BETWEEN 1 AND 10), -- Rate of Perceived Exertion
  rir INTEGER CHECK (rir BETWEEN 0 AND 5), -- Reps in Reserve
  tempo TEXT, -- e.g., '3-1-2-0' (eccentric-pause-concentric-pause)
  rest_seconds INTEGER,
  is_pr BOOLEAN DEFAULT FALSE,
  is_failure BOOLEAN DEFAULT FALSE,
  distance_meters DECIMAL(10,2), -- for cardio exercises
  time_seconds INTEGER, -- for timed sets/holds
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sets_workout ON workout_sets(workout_id);
CREATE INDEX idx_sets_exercise ON workout_sets(exercise_id);
CREATE INDEX idx_sets_pr ON workout_sets(is_pr) WHERE is_pr = TRUE;


-- ============================================================
-- TABLE: personal_records
-- Track PRs separately for quick access
-- ============================================================

CREATE TABLE personal_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  exercise_id UUID NOT NULL REFERENCES exercises(id) ON DELETE CASCADE,
  record_type TEXT NOT NULL CHECK (record_type IN (
    '1rm', '3rm', '5rm', '8rm', '10rm', 
    'max_reps_bodyweight', 'max_volume_single_set', 'max_weight'
  )),
  value DECIMAL(8,2) NOT NULL, -- weight in kg or reps
  achieved_date DATE NOT NULL,
  workout_id UUID REFERENCES workouts(id) ON DELETE SET NULL,
  previous_value DECIMAL(8,2),
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  
  -- Unique constraint: one record per athlete/exercise/type
  UNIQUE(athlete_id, exercise_id, record_type)
);

CREATE INDEX idx_prs_athlete ON personal_records(athlete_id);


-- ============================================================
-- TABLE: body_metrics
-- Daily/weekly body composition tracking
-- ============================================================

CREATE TABLE body_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  measured_at DATE NOT NULL,
  weight_kg DECIMAL(5,2),
  body_fat_pct DECIMAL(4,1),
  lean_mass_kg DECIMAL(5,2), -- computed or from DEXA
  -- Circumference measurements in cm
  waist_cm DECIMAL(5,1),
  chest_cm DECIMAL(5,1),
  shoulders_cm DECIMAL(5,1),
  left_arm_cm DECIMAL(4,1),
  right_arm_cm DECIMAL(4,1),
  left_thigh_cm DECIMAL(5,1),
  right_thigh_cm DECIMAL(5,1),
  left_calf_cm DECIMAL(4,1),
  right_calf_cm DECIMAL(4,1),
  neck_cm DECIMAL(4,1),
  hip_cm DECIMAL(5,1),
  -- Performance metrics
  resting_heart_rate INTEGER,
  blood_pressure_systolic INTEGER,
  blood_pressure_diastolic INTEGER,
  -- Recovery
  sleep_quality INTEGER CHECK (sleep_quality BETWEEN 1 AND 10),
  stress_level INTEGER CHECK (stress_level BETWEEN 1 AND 10),
  recovery_score INTEGER CHECK (recovery_score BETWEEN 1 AND 100),
  steps INTEGER,
  notes TEXT,
  photo_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  
  UNIQUE(athlete_id, measured_at)
);

CREATE INDEX idx_metrics_athlete_date ON body_metrics(athlete_id, measured_at DESC);


-- ============================================================
-- TABLE: nutrition_logs
-- Daily macro/calorie tracking
-- ============================================================

CREATE TABLE nutrition_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  log_date DATE NOT NULL,
  meal_type meal_type NOT NULL,
  meal_name TEXT,
  calories INTEGER NOT NULL CHECK (calories >= 0),
  protein_g DECIMAL(6,1) CHECK (protein_g >= 0),
  carbs_g DECIMAL(6,1) CHECK (carbs_g >= 0),
  fats_g DECIMAL(6,1) CHECK (fats_g >= 0),
  fiber_g DECIMAL(5,1),
  sodium_mg INTEGER,
  sugar_g DECIMAL(5,1),
  water_ml INTEGER, -- water intake per meal/period
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_nutrition_athlete_date ON nutrition_logs(athlete_id, log_date DESC);
CREATE INDEX idx_nutrition_meal ON nutrition_logs(meal_type);


-- ============================================================
-- TABLE: cardio_sessions
-- Separate cardio tracking
-- ============================================================

CREATE TABLE cardio_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  workout_id UUID REFERENCES workouts(id) ON DELETE SET NULL,
  session_date DATE NOT NULL,
  cardio_type cardio_type NOT NULL,
  activity TEXT NOT NULL, -- 'treadmill', 'stairmaster', 'cycling', etc.
  duration_minutes INTEGER NOT NULL,
  distance_km DECIMAL(6,2),
  avg_heart_rate INTEGER,
  max_heart_rate INTEGER,
  calories_burned INTEGER,
  avg_pace_min_per_km DECIMAL(5,2),
  incline_pct DECIMAL(4,1),
  resistance_level INTEGER,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_cardio_athlete_date ON cardio_sessions(athlete_id, session_date DESC);


-- ============================================================
-- TABLE: supplement_log
-- Track supplement intake
-- ============================================================

CREATE TABLE supplement_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athlete_id UUID NOT NULL REFERENCES athletes(id) ON DELETE CASCADE,
  log_date DATE NOT NULL,
  supplement_name TEXT NOT NULL,
  dosage TEXT NOT NULL, -- e.g., '5g', '200mg', '1 scoop'
  timing TEXT, -- 'pre_workout', 'post_workout', 'morning', 'evening'
  brand TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_supplements_athlete_date ON supplement_log(athlete_id, log_date DESC);


-- ============================================================
-- TABLE: workout_tags
-- Flexible tagging system for workouts
-- ============================================================

CREATE TABLE workout_tags (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT UNIQUE NOT NULL,
  color TEXT DEFAULT '#6366f1', -- hex color for UI
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE workout_tag_assignments (
  workout_id UUID NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
  tag_id UUID NOT NULL REFERENCES workout_tags(id) ON DELETE CASCADE,
  PRIMARY KEY (workout_id, tag_id)
);


-- ============================================================
-- TRIGGER: Auto-update updated_at timestamps
-- This is a common Supabase pattern
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_athletes_updated_at
  BEFORE UPDATE ON athletes
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER set_programs_updated_at
  BEFORE UPDATE ON workout_programs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- ============================================================
-- SUCCESS MESSAGE
-- ============================================================

DO $$ BEGIN RAISE NOTICE '✅ Schema created successfully! Run 02_seed_exercises.sql next.'; END $$;
