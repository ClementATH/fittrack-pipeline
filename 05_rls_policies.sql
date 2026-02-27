-- ============================================================
-- FITTRACK PRO - Supabase Fitness Database
-- File 5 of 5: ROW LEVEL SECURITY (RLS) + REALTIME
-- 
-- Paste this FIFTH (and last) into Supabase SQL Editor
-- 
-- ⚠️  NOTE: RLS is OFF by default in Supabase.
-- These policies only take effect when RLS is ENABLED.
-- In your Supabase dashboard: Table Editor > [table] > RLS toggle
--
-- For learning/testing, you can keep RLS disabled and use
-- the Supabase dashboard freely. Enable RLS when you build
-- a real app with authentication.
-- ============================================================


-- ============================================================
-- ENABLE RLS ON ALL TABLES
-- Once enabled, NO data is accessible unless a policy allows it
-- ============================================================

ALTER TABLE athletes ENABLE ROW LEVEL SECURITY;
ALTER TABLE exercises ENABLE ROW LEVEL SECURITY;
ALTER TABLE workout_programs ENABLE ROW LEVEL SECURITY;
ALTER TABLE workouts ENABLE ROW LEVEL SECURITY;
ALTER TABLE workout_sets ENABLE ROW LEVEL SECURITY;
ALTER TABLE personal_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE body_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE nutrition_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE cardio_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE supplement_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE workout_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE workout_tag_assignments ENABLE ROW LEVEL SECURITY;


-- ============================================================
-- HELPER: Link Supabase Auth to our athletes table
-- In production, you'd add: auth_user_id UUID REFERENCES auth.users(id)
-- For now, we'll create policies assuming this link exists
-- ============================================================

-- This function gets the current logged-in user's athlete ID
-- In production: match auth.uid() to athletes.auth_user_id
CREATE OR REPLACE FUNCTION get_my_athlete_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  -- In production, replace with:
  -- SELECT id FROM athletes WHERE auth_user_id = auth.uid();
  -- For testing, returns the sample athlete:
  SELECT 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'::UUID;
$$;


-- ============================================================
-- RLS POLICIES: Athletes Table
-- Users can only see/edit their own profile
-- ============================================================

-- SELECT: Users see only their own profile
CREATE POLICY "Athletes: users can view own profile"
  ON athletes FOR SELECT
  USING (id = get_my_athlete_id());

-- UPDATE: Users can only update their own profile
CREATE POLICY "Athletes: users can update own profile"
  ON athletes FOR UPDATE
  USING (id = get_my_athlete_id())
  WITH CHECK (id = get_my_athlete_id());

-- INSERT: Anyone authenticated can create their profile
CREATE POLICY "Athletes: authenticated users can create profile"
  ON athletes FOR INSERT
  WITH CHECK (TRUE); -- In production: WITH CHECK (auth.uid() IS NOT NULL)


-- ============================================================
-- RLS POLICIES: Exercises Table
-- Everyone can read, only creators can edit custom exercises
-- ============================================================

-- SELECT: Everyone can view the exercise library
CREATE POLICY "Exercises: public read access"
  ON exercises FOR SELECT
  USING (TRUE);

-- INSERT: Users can add custom exercises
CREATE POLICY "Exercises: users can add custom exercises"
  ON exercises FOR INSERT
  WITH CHECK (is_custom = TRUE AND created_by = get_my_athlete_id());

-- UPDATE: Only creator can edit custom exercises
CREATE POLICY "Exercises: creators can edit own custom exercises"
  ON exercises FOR UPDATE
  USING (is_custom = TRUE AND created_by = get_my_athlete_id());

-- DELETE: Only creator can delete custom exercises
CREATE POLICY "Exercises: creators can delete own custom exercises"
  ON exercises FOR DELETE
  USING (is_custom = TRUE AND created_by = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Workout Programs
-- Private to the athlete who created them
-- ============================================================

CREATE POLICY "Programs: users can view own programs"
  ON workout_programs FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Programs: users can create own programs"
  ON workout_programs FOR INSERT
  WITH CHECK (athlete_id = get_my_athlete_id());

CREATE POLICY "Programs: users can update own programs"
  ON workout_programs FOR UPDATE
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Programs: users can delete own programs"
  ON workout_programs FOR DELETE
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Workouts
-- Private to athlete
-- ============================================================

CREATE POLICY "Workouts: users can view own workouts"
  ON workouts FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Workouts: users can create own workouts"
  ON workouts FOR INSERT
  WITH CHECK (athlete_id = get_my_athlete_id());

CREATE POLICY "Workouts: users can update own workouts"
  ON workouts FOR UPDATE
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Workouts: users can delete own workouts"
  ON workouts FOR DELETE
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Workout Sets
-- Access through parent workout ownership
-- ============================================================

CREATE POLICY "Sets: users can view own workout sets"
  ON workout_sets FOR SELECT
  USING (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );

CREATE POLICY "Sets: users can create sets in own workouts"
  ON workout_sets FOR INSERT
  WITH CHECK (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );

CREATE POLICY "Sets: users can update own workout sets"
  ON workout_sets FOR UPDATE
  USING (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );

CREATE POLICY "Sets: users can delete own workout sets"
  ON workout_sets FOR DELETE
  USING (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );


-- ============================================================
-- RLS POLICIES: Personal Records
-- ============================================================

CREATE POLICY "PRs: users can view own PRs"
  ON personal_records FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "PRs: users can manage own PRs"
  ON personal_records FOR ALL
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Body Metrics (SENSITIVE - strict privacy)
-- ============================================================

CREATE POLICY "Metrics: users can view own body metrics"
  ON body_metrics FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Metrics: users can manage own body metrics"
  ON body_metrics FOR ALL
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Nutrition Logs
-- ============================================================

CREATE POLICY "Nutrition: users can view own logs"
  ON nutrition_logs FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Nutrition: users can manage own logs"
  ON nutrition_logs FOR ALL
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Cardio Sessions
-- ============================================================

CREATE POLICY "Cardio: users can view own sessions"
  ON cardio_sessions FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Cardio: users can manage own sessions"
  ON cardio_sessions FOR ALL
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Supplement Log
-- ============================================================

CREATE POLICY "Supplements: users can view own logs"
  ON supplement_log FOR SELECT
  USING (athlete_id = get_my_athlete_id());

CREATE POLICY "Supplements: users can manage own logs"
  ON supplement_log FOR ALL
  USING (athlete_id = get_my_athlete_id());


-- ============================================================
-- RLS POLICIES: Tags (public read, admin write)
-- ============================================================

CREATE POLICY "Tags: public read"
  ON workout_tags FOR SELECT
  USING (TRUE);

CREATE POLICY "Tag assignments: users can view own"
  ON workout_tag_assignments FOR SELECT
  USING (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );

CREATE POLICY "Tag assignments: users can manage own"
  ON workout_tag_assignments FOR ALL
  USING (
    workout_id IN (
      SELECT id FROM workouts WHERE athlete_id = get_my_athlete_id()
    )
  );


-- ============================================================
-- REALTIME: Enable realtime subscriptions
-- This lets your frontend listen for live changes
-- 
-- In your app:
-- supabase.channel('workouts')
--   .on('postgres_changes', { event: '*', schema: 'public', table: 'workouts' }, callback)
--   .subscribe()
-- ============================================================

-- Enable realtime on key tables
-- (In Supabase dashboard: Database > Replication > enable tables)
-- These ALTER statements simulate enabling realtime via SQL:

ALTER PUBLICATION supabase_realtime ADD TABLE workouts;
ALTER PUBLICATION supabase_realtime ADD TABLE workout_sets;
ALTER PUBLICATION supabase_realtime ADD TABLE body_metrics;
ALTER PUBLICATION supabase_realtime ADD TABLE personal_records;


-- ============================================================
-- BONUS: Create a service_role bypass policy
-- The service_role key bypasses RLS (for admin/backend use)
-- This is already built into Supabase - just use the 
-- service_role key instead of the anon key in your server code
-- ============================================================


DO $$ BEGIN RAISE NOTICE '
✅ RLS Policies and Realtime configured!

============================================
  DATABASE SETUP COMPLETE! 🎉
============================================

Your FitTrack Pro database now contains:
  📊 12 tables with proper relationships
  🏋️ 60+ exercises in the library  
  💪 72 workouts across 12 weeks
  📈 4,000+ individual set records
  ⚖️  84 days of body composition data
  🥗 504 nutrition log entries
  💊 504 supplement log entries  
  🏃 36 cardio sessions
  🏆 10 personal records
  👁️ 5 analytics views
  ⚡ 6 callable RPC functions
  🔒 Full RLS policy coverage
  📡 Realtime enabled on key tables

Next steps:
  1. Explore tables in Supabase Table Editor
  2. Run queries in SQL Editor (see README)
  3. Try the RPC functions
  4. Build a frontend with the Supabase JS client
  5. Connect via Claude Code for data engineering!
'; END $$;
