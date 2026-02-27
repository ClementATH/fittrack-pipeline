-- ============================================================
-- FITTRACK PRO - Supabase Fitness Database
-- File 3 of 5: SEED ATHLETE + 12 WEEKS OF TRAINING DATA
-- 
-- Paste this THIRD into Supabase SQL Editor
-- Creates a lean athlete with 12 weeks of PPL training data
-- ============================================================

-- ============================================================
-- ATHLETE PROFILE
-- ============================================================

INSERT INTO athletes (
  id, email, username, full_name, date_of_birth, gender,
  height_cm, target_weight_kg, activity_level, training_experience_years,
  bio, instagram_handle, unit_preference
) VALUES (
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  'marcus.chen@email.com',
  'marcuslifts',
  'Marcus Chen',
  '1997-03-15',
  'male',
  180.3,      -- ~5'11"
  81.6,       -- ~180 lbs target
  'very_active',
  6,
  'Natural lean athlete. PPL enthusiast. Chasing strength and aesthetics. 6 years of consistent training.',
  '@marcuslifts',
  'imperial'
);


-- ============================================================
-- WORKOUT TAGS
-- ============================================================

INSERT INTO workout_tags (id, name, color) VALUES
  ('11111111-1111-1111-1111-111111111101', 'Push', '#ef4444'),
  ('11111111-1111-1111-1111-111111111102', 'Pull', '#3b82f6'),
  ('11111111-1111-1111-1111-111111111103', 'Legs', '#22c55e'),
  ('11111111-1111-1111-1111-111111111104', 'Deload', '#f59e0b'),
  ('11111111-1111-1111-1111-111111111105', 'PR Day', '#a855f7'),
  ('11111111-1111-1111-1111-111111111106', 'High Volume', '#ec4899'),
  ('11111111-1111-1111-1111-111111111107', 'Strength Focus', '#6366f1'),
  ('11111111-1111-1111-1111-111111111108', 'Recovery', '#14b8a6');


-- ============================================================
-- WORKOUT PROGRAM - 12 Week Push/Pull/Legs Hypertrophy
-- ============================================================

INSERT INTO workout_programs (
  id, athlete_id, name, description, goal, split,
  duration_weeks, days_per_week, is_active, start_date, end_date,
  deload_week, notes
) VALUES (
  'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01',
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  '12-Week Lean Mass PPL',
  'Progressive overload focused PPL split. Weeks 1-3 base volume, weeks 4-7 increased intensity, weeks 8-11 peak volume, week 12 deload.',
  'hypertrophy',
  'push_pull_legs',
  12,
  6,
  TRUE,
  '2025-10-06',
  '2025-12-28',
  12,
  'Running slight surplus of +200 cals. Focus on 6-12 rep range for compounds, 10-15 for isolations. RPE 7-9 on working sets.'
);


-- ============================================================
-- SUPPLEMENT LOG (consistent daily stack)
-- ============================================================

-- Generate 84 days of supplement data (12 weeks)
INSERT INTO supplement_log (athlete_id, log_date, supplement_name, dosage, timing, brand)
SELECT
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  d::date,
  s.name,
  s.dose,
  s.timing,
  s.brand
FROM generate_series('2025-10-06'::date, '2025-12-28'::date, '1 day'::interval) d
CROSS JOIN (VALUES
  ('Creatine Monohydrate', '5g', 'morning', 'Thorne'),
  ('Whey Protein Isolate', '30g (1 scoop)', 'post_workout', 'Transparent Labs'),
  ('Vitamin D3', '5000 IU', 'morning', 'NOW Foods'),
  ('Fish Oil', '2g EPA/DHA', 'morning', 'Nordic Naturals'),
  ('Magnesium Glycinate', '400mg', 'evening', 'Pure Encapsulations'),
  ('Zinc', '30mg', 'evening', 'Thorne')
) AS s(name, dose, timing, brand);


-- ============================================================
-- HELPER: Create all 72 workouts (6 days/week x 12 weeks)
-- We'll use a DO block for the complex data generation
-- ============================================================

DO $$
DECLARE
  v_athlete_id UUID := 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
  v_program_id UUID := 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01';
  v_week INTEGER;
  v_day INTEGER;
  v_workout_id UUID;
  v_date DATE;
  v_day_name TEXT;
  v_start_time TIMESTAMPTZ;
  v_end_time TIMESTAMPTZ;
  v_energy INTEGER;
  v_pump INTEGER;
  v_sleep DECIMAL;
  v_base_date DATE := '2025-10-06';
  v_duration_mins INTEGER;
  v_status workout_status;
  v_tag_id UUID;
BEGIN

  -- Loop through 12 weeks, 6 days each
  FOR v_week IN 1..12 LOOP
    FOR v_day IN 1..6 LOOP
      
      v_workout_id := gen_random_uuid();
      
      -- Calculate date (skip Sundays = rest day)
      v_date := v_base_date + ((v_week - 1) * 7) + (v_day - 1);
      -- Skip if its a Sunday (day 7 would be rest)
      IF EXTRACT(DOW FROM v_date) = 0 THEN
        v_date := v_date + 1;
      END IF;
      
      -- Determine day name based on PPL rotation
      CASE ((v_day - 1) % 6)
        WHEN 0 THEN v_day_name := 'Push A - Chest Focus';
        WHEN 1 THEN v_day_name := 'Pull A - Back Width';
        WHEN 2 THEN v_day_name := 'Legs A - Quad Focus';
        WHEN 3 THEN v_day_name := 'Push B - Shoulder Focus';
        WHEN 4 THEN v_day_name := 'Pull B - Back Thickness';
        WHEN 5 THEN v_day_name := 'Legs B - Hamstring/Glute Focus';
      END CASE;
      
      -- Week 12 deload = lighter sessions
      IF v_week = 12 THEN
        v_day_name := 'DELOAD - ' || v_day_name;
      END IF;
      
      -- Simulate realistic energy/pump/sleep variations
      v_energy := 5 + floor(random() * 5)::int; -- 5-9
      v_pump := 5 + floor(random() * 5)::int;
      v_sleep := 5.5 + (random() * 3.5)::decimal(3,1); -- 5.5-9.0
      v_duration_mins := 55 + floor(random() * 35)::int; -- 55-90 min
      
      -- Most workouts completed, occasional skip
      IF random() < 0.04 THEN -- ~4% skip rate
        v_status := 'skipped';
      ELSE
        v_status := 'completed';
      END IF;
      
      -- Start time between 5:30 AM and 7:00 AM (early morning lifter)
      v_start_time := (v_date || ' 05:30:00')::timestamptz + (random() * interval '90 minutes');
      v_end_time := v_start_time + (v_duration_mins || ' minutes')::interval;
      
      -- Insert workout
      INSERT INTO workouts (
        id, athlete_id, program_id, workout_date, day_name, status,
        start_time, end_time, energy_level, pump_rating,
        sleep_hours_prior, notes, location
      ) VALUES (
        v_workout_id, v_athlete_id, v_program_id, v_date, v_day_name,
        v_status, v_start_time, v_end_time,
        v_energy, v_pump, v_sleep,
        CASE 
          WHEN v_week = 12 THEN 'Deload week - keeping intensity low, focusing on form'
          WHEN v_energy >= 9 THEN 'Feeling amazing today. Hit everything hard.'
          WHEN v_energy <= 6 THEN 'Low energy but showed up. Ground through it.'
          WHEN v_pump >= 9 THEN 'Insane pump today. Best session this week.'
          ELSE NULL
        END,
        'gym'
      );
      
      -- Tag the workout
      CASE ((v_day - 1) % 6)
        WHEN 0 THEN v_tag_id := '11111111-1111-1111-1111-111111111101'; -- Push
        WHEN 1 THEN v_tag_id := '11111111-1111-1111-1111-111111111102'; -- Pull
        WHEN 2 THEN v_tag_id := '11111111-1111-1111-1111-111111111103'; -- Legs
        WHEN 3 THEN v_tag_id := '11111111-1111-1111-1111-111111111101'; -- Push
        WHEN 4 THEN v_tag_id := '11111111-1111-1111-1111-111111111102'; -- Pull
        WHEN 5 THEN v_tag_id := '11111111-1111-1111-1111-111111111103'; -- Legs
      END CASE;
      
      INSERT INTO workout_tag_assignments (workout_id, tag_id)
      VALUES (v_workout_id, v_tag_id);
      
      -- Deload tag for week 12
      IF v_week = 12 THEN
        INSERT INTO workout_tag_assignments (workout_id, tag_id)
        VALUES (v_workout_id, '11111111-1111-1111-1111-111111111104');
      END IF;

      -- ============================================================
      -- INSERT SETS FOR EACH WORKOUT DAY
      -- Progressive overload: weight increases ~2.5% every 3 weeks
      -- ============================================================
      
      IF v_status = 'completed' THEN
      
        -- PUSH A - Chest Focus
        IF (v_day - 1) % 6 = 0 THEN
          -- Barbell Bench Press: 4 working sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rir, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 
            CASE WHEN s.num = 1 THEN 'warmup'::set_type ELSE 'working'::set_type END,
            CASE WHEN v_week <= 11 
              THEN ROUND((CASE WHEN s.num = 1 THEN 60 ELSE 88 + (v_week * 1.5) END)::numeric, 1)
              ELSE ROUND((CASE WHEN s.num = 1 THEN 40 ELSE 65 END)::numeric, 1) -- deload
            END,
            CASE WHEN s.num = 1 THEN 12 ELSE 8 - floor(random()*2)::int END,
            CASE WHEN s.num = 1 THEN 4 ELSE 7.5 + (v_week * 0.15) END,
            CASE WHEN s.num = 1 THEN 4 ELSE 3 - floor(v_week/4)::int END,
            CASE WHEN s.num = 1 THEN 60 ELSE 180 END
          FROM exercises e, generate_series(1,5) AS s(num)
          WHERE e.slug = 'barbell-bench-press';

          -- Incline DB Press: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((32 + (v_week * 1.0))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0 + (v_week * 0.1),
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'incline-dumbbell-press';

          -- Cable Flyes: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((15 + (v_week * 0.5))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'cable-flyes';

          -- Dumbbell Shoulder Press: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((27 + (v_week * 0.8))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'dumbbell-shoulder-press';

          -- Lateral Raises: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((10 + (v_week * 0.3))::numeric, 1),
            15 - floor(random()*3)::int,
            8.5,
            60
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'lateral-raises';

          -- Tricep Pushdown: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((30 + (v_week * 0.8))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'tricep-pushdown';

          -- Skull Crushers: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((25 + (v_week * 0.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'skull-crushers';

        -- PULL A - Back Width
        ELSIF (v_day - 1) % 6 = 1 THEN
          -- Pull-Ups: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds, notes)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((10 + (v_week * 1.0))::numeric, 1), -- added weight
            8 - floor(random()*2)::int,
            8.5,
            150,
            'Weighted pull-ups +' || ROUND((10 + (v_week * 1.0))::numeric, 1) || 'kg'
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'pull-ups';

          -- Lat Pulldown: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((60 + (v_week * 1.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'lat-pulldown';

          -- Seated Cable Row: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((65 + (v_week * 1.2))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'seated-cable-row';

          -- Face Pulls: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((18 + (v_week * 0.3))::numeric, 1),
            18 - floor(random()*4)::int,
            7.5,
            60
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'face-pulls';

          -- Barbell Curl: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((32 + (v_week * 0.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'barbell-curl';

          -- Hammer Curls: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((16 + (v_week * 0.4))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            60
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'hammer-curls';

        -- LEGS A - Quad Focus
        ELSIF (v_day - 1) % 6 = 2 THEN
          -- Barbell Back Squat: 1 warmup + 4 working
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rir, rest_seconds)
          SELECT v_workout_id, e.id, s.num,
            CASE WHEN s.num = 1 THEN 'warmup'::set_type ELSE 'working'::set_type END,
            CASE WHEN s.num = 1 THEN 60 ELSE ROUND((100 + (v_week * 2.0))::numeric, 1) END,
            CASE WHEN s.num = 1 THEN 10 ELSE 6 - floor(random()*2)::int END,
            CASE WHEN s.num = 1 THEN 4 ELSE 8.0 + (v_week * 0.15) END,
            CASE WHEN s.num = 1 THEN 4 ELSE 2 END,
            CASE WHEN s.num = 1 THEN 90 ELSE 210 END
          FROM exercises e, generate_series(1,5) AS s(num)
          WHERE e.slug = 'barbell-back-squat';

          -- Hack Squat: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((80 + (v_week * 2.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            150
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'hack-squat';

          -- Leg Extension: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((50 + (v_week * 1.0))::numeric, 1),
            12 - floor(random()*3)::int,
            8.5,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'leg-extension';

          -- Walking Lunges: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((20 + (v_week * 0.5))::numeric, 1),
            12, -- per leg
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'walking-lunges';

          -- Standing Calf Raise: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((80 + (v_week * 1.5))::numeric, 1),
            15 - floor(random()*3)::int,
            8.0,
            60
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'standing-calf-raise';

        -- PUSH B - Shoulder Focus
        ELSIF (v_day - 1) % 6 = 3 THEN
          -- Overhead Press: 1 warmup + 4 working
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num,
            CASE WHEN s.num = 1 THEN 'warmup'::set_type ELSE 'working'::set_type END,
            CASE WHEN s.num = 1 THEN 30 ELSE ROUND((55 + (v_week * 1.0))::numeric, 1) END,
            CASE WHEN s.num = 1 THEN 12 ELSE 6 - floor(random()*2)::int END,
            CASE WHEN s.num = 1 THEN 4 ELSE 8.0 + (v_week * 0.15) END,
            CASE WHEN s.num = 1 THEN 60 ELSE 180 END
          FROM exercises e, generate_series(1,5) AS s(num)
          WHERE e.slug = 'overhead-press';

          -- Arnold Press: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((22 + (v_week * 0.6))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'arnold-press';

          -- Cable Lateral Raise: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((8 + (v_week * 0.3))::numeric, 1),
            15 - floor(random()*3)::int,
            8.5,
            60
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'cable-lateral-raise';

          -- Incline Barbell Bench: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((70 + (v_week * 1.2))::numeric, 1),
            8 - floor(random()*2)::int,
            8.0,
            150
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'incline-barbell-bench-press';

          -- Low Cable Fly: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((12 + (v_week * 0.4))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'low-cable-fly';

          -- Overhead Tricep Extension: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((28 + (v_week * 0.6))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'overhead-tricep-extension';

          -- Rear Delt Flyes: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((10 + (v_week * 0.3))::numeric, 1),
            15 - floor(random()*3)::int,
            7.5,
            60
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'rear-delt-flyes';

        -- PULL B - Back Thickness
        ELSIF (v_day - 1) % 6 = 4 THEN
          -- Barbell Row: 1 warmup + 4 working
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num,
            CASE WHEN s.num = 1 THEN 'warmup'::set_type ELSE 'working'::set_type END,
            CASE WHEN s.num = 1 THEN 50 ELSE ROUND((80 + (v_week * 1.5))::numeric, 1) END,
            CASE WHEN s.num = 1 THEN 12 ELSE 8 - floor(random()*2)::int END,
            CASE WHEN s.num = 1 THEN 4 ELSE 8.0 + (v_week * 0.15) END,
            CASE WHEN s.num = 1 THEN 90 ELSE 150 END
          FROM exercises e, generate_series(1,5) AS s(num)
          WHERE e.slug = 'barbell-row';

          -- T-Bar Row: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((50 + (v_week * 1.2))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 't-bar-row';

          -- Chest Supported Row: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((28 + (v_week * 0.6))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'chest-supported-row';

          -- Face Pulls: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((18 + (v_week * 0.3))::numeric, 1),
            18 - floor(random()*4)::int,
            7.5,
            60
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'face-pulls';

          -- Incline DB Curl: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((12 + (v_week * 0.3))::numeric, 1),
            10 - floor(random()*2)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'incline-dumbbell-curl';

          -- Preacher Curl: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((25 + (v_week * 0.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            90
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'preacher-curl';

        -- LEGS B - Hamstring/Glute Focus
        ELSIF (v_day - 1) % 6 = 5 THEN
          -- Romanian Deadlift: 1 warmup + 4 working
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num,
            CASE WHEN s.num = 1 THEN 'warmup'::set_type ELSE 'working'::set_type END,
            CASE WHEN s.num = 1 THEN 60 ELSE ROUND((95 + (v_week * 1.5))::numeric, 1) END,
            CASE WHEN s.num = 1 THEN 12 ELSE 8 - floor(random()*2)::int END,
            CASE WHEN s.num = 1 THEN 4 ELSE 8.0 + (v_week * 0.15) END,
            CASE WHEN s.num = 1 THEN 90 ELSE 180 END
          FROM exercises e, generate_series(1,5) AS s(num)
          WHERE e.slug = 'romanian-deadlift';

          -- Hip Thrust: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((100 + (v_week * 2.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            150
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'hip-thrust';

          -- Bulgarian Split Squat: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((22 + (v_week * 0.5))::numeric, 1),
            10 - floor(random()*2)::int,
            8.5,
            120
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'bulgarian-split-squat';

          -- Leg Curl: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((45 + (v_week * 1.0))::numeric, 1),
            12 - floor(random()*3)::int,
            8.0,
            90
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'leg-curl';

          -- Seated Calf Raise: 4 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, weight_kg, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            ROUND((50 + (v_week * 1.0))::numeric, 1),
            15 - floor(random()*3)::int,
            8.0,
            60
          FROM exercises e, generate_series(1,4) AS s(num)
          WHERE e.slug = 'seated-calf-raise';

          -- Hanging Leg Raise: 3 sets
          INSERT INTO workout_sets (workout_id, exercise_id, set_number, set_type, reps, rpe, rest_seconds)
          SELECT v_workout_id, e.id, s.num, 'working',
            15 - floor(random()*3)::int,
            8.0,
            60
          FROM exercises e, generate_series(1,3) AS s(num)
          WHERE e.slug = 'hanging-leg-raise';

        END IF;
      END IF; -- end status check

    END LOOP; -- end day loop
  END LOOP; -- end week loop

END $$;


-- ============================================================
-- UPDATE: Calculate total volume per workout
-- Volume = sum of (weight * reps) for all working sets
-- ============================================================

UPDATE workouts w
SET total_volume_kg = sub.vol,
    total_sets = sub.sets
FROM (
  SELECT
    ws.workout_id,
    ROUND(SUM(COALESCE(ws.weight_kg, 0) * COALESCE(ws.reps, 0))::numeric, 2) AS vol,
    COUNT(*) FILTER (WHERE ws.set_type != 'warmup') AS sets
  FROM workout_sets ws
  GROUP BY ws.workout_id
) sub
WHERE w.id = sub.workout_id;


-- ============================================================
-- BODY METRICS: 12 weeks of daily weigh-ins + weekly measurements
-- Simulating a lean bulk: ~0.15kg/week gain
-- ============================================================

INSERT INTO body_metrics (
  athlete_id, measured_at, weight_kg, body_fat_pct, lean_mass_kg,
  waist_cm, chest_cm, shoulders_cm,
  left_arm_cm, right_arm_cm, left_thigh_cm, right_thigh_cm,
  left_calf_cm, right_calf_cm, neck_cm, hip_cm,
  resting_heart_rate, sleep_quality, stress_level, recovery_score, steps
)
SELECT
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  d::date,
  -- Weight: starts 79.5kg, gains ~0.15kg/week with daily fluctuation
  ROUND((79.5 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 7 * 0.15) + (random() * 1.2 - 0.6))::numeric, 1),
  -- Body fat: stays lean 11-12.5%
  ROUND((11.0 + (random() * 1.5))::numeric, 1),
  -- Lean mass (calculated rough)
  ROUND((70.8 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 7 * 0.12))::numeric, 1),
  -- Measurements only on Mondays (weekly)
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((79.5 + random() * 1.5)::numeric, 1) ELSE NULL END, -- waist
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((101 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 1.5) + random() * 0.5)::numeric, 1) ELSE NULL END, -- chest
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((122 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 1.0) + random() * 0.5)::numeric, 1) ELSE NULL END, -- shoulders
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((38.0 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 0.8) + random() * 0.3)::numeric, 1) ELSE NULL END, -- L arm
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((38.2 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 0.8) + random() * 0.3)::numeric, 1) ELSE NULL END, -- R arm
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((59.0 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 0.5) + random() * 0.5)::numeric, 1) ELSE NULL END, -- L thigh
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((59.3 + (EXTRACT(DAY FROM d - '2025-10-06'::date)::numeric / 84 * 0.5) + random() * 0.5)::numeric, 1) ELSE NULL END, -- R thigh
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((38.5 + random() * 0.3)::numeric, 1) ELSE NULL END, -- L calf
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((38.7 + random() * 0.3)::numeric, 1) ELSE NULL END, -- R calf
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((38.0 + random() * 0.5)::numeric, 1) ELSE NULL END, -- neck
  CASE WHEN EXTRACT(DOW FROM d) = 1 THEN ROUND((92.5 + random() * 1.0)::numeric, 1) ELSE NULL END, -- hip
  -- Daily vitals
  (52 + floor(random() * 10))::int, -- resting HR 52-61
  (5 + floor(random() * 5))::int, -- sleep quality 5-9
  (2 + floor(random() * 5))::int, -- stress 2-6
  (60 + floor(random() * 35))::int, -- recovery 60-94
  (6000 + floor(random() * 8000))::int -- steps 6k-14k
FROM generate_series('2025-10-06'::date, '2025-12-28'::date, '1 day'::interval) d;


-- ============================================================
-- NUTRITION: Daily macro tracking (clean lean bulk diet)
-- ~2,700 cal on training days, ~2,400 on rest days
-- Protein 200g+, moderate carbs, moderate fats
-- ============================================================

INSERT INTO nutrition_logs (
  athlete_id, log_date, meal_type, meal_name,
  calories, protein_g, carbs_g, fats_g, fiber_g, water_ml
)
SELECT
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  d::date,
  m.meal,
  m.name,
  m.cals + floor(random() * 80 - 40)::int, -- ±40 cal variation
  ROUND((m.pro + (random() * 10 - 5))::numeric, 1),
  ROUND((m.carb + (random() * 15 - 7))::numeric, 1),
  ROUND((m.fat + (random() * 5 - 2.5))::numeric, 1),
  ROUND((m.fib + (random() * 3))::numeric, 1),
  m.water
FROM generate_series('2025-10-06'::date, '2025-12-28'::date, '1 day'::interval) d
CROSS JOIN (VALUES
  ('breakfast'::meal_type, 'Egg whites + oats + berries', 520, 42.0, 55.0, 12.0, 6.0, 500),
  ('pre_workout'::meal_type, 'Rice + chicken breast + veggies', 580, 48.0, 65.0, 8.0, 5.0, 400),
  ('post_workout'::meal_type, 'Whey shake + banana + PB', 450, 45.0, 40.0, 12.0, 4.0, 600),
  ('lunch'::meal_type, 'Ground turkey + sweet potato + broccoli', 620, 50.0, 55.0, 18.0, 8.0, 500),
  ('dinner'::meal_type, 'Salmon + jasmine rice + asparagus', 580, 42.0, 50.0, 20.0, 6.0, 400),
  ('snack'::meal_type, 'Greek yogurt + almonds + honey', 280, 22.0, 20.0, 12.0, 2.0, 300)
) AS m(meal, name, cals, pro, carb, fat, fib, water);


-- ============================================================
-- CARDIO: 3x/week LISS + occasional HIIT
-- ============================================================

INSERT INTO cardio_sessions (
  athlete_id, session_date, cardio_type, activity,
  duration_minutes, avg_heart_rate, max_heart_rate,
  calories_burned, incline_pct, notes
)
SELECT
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  d::date,
  CASE WHEN EXTRACT(DOW FROM d) = 3 THEN 'hiit'::cardio_type ELSE 'liss'::cardio_type END,
  CASE
    WHEN EXTRACT(DOW FROM d) = 1 THEN 'incline treadmill walk'
    WHEN EXTRACT(DOW FROM d) = 3 THEN 'stairmaster intervals'
    WHEN EXTRACT(DOW FROM d) = 5 THEN 'incline treadmill walk'
    ELSE 'stationary bike'
  END,
  CASE WHEN EXTRACT(DOW FROM d) = 3 THEN 20 ELSE 30 END, -- HIIT shorter
  CASE WHEN EXTRACT(DOW FROM d) = 3 THEN 155 + floor(random()*15)::int ELSE 125 + floor(random()*10)::int END,
  CASE WHEN EXTRACT(DOW FROM d) = 3 THEN 175 + floor(random()*10)::int ELSE 145 + floor(random()*10)::int END,
  CASE WHEN EXTRACT(DOW FROM d) = 3 THEN 250 + floor(random()*50)::int ELSE 200 + floor(random()*40)::int END,
  CASE WHEN EXTRACT(DOW FROM d) != 3 THEN 10.0 + (random()*5)::decimal(4,1) ELSE NULL END,
  CASE
    WHEN EXTRACT(DOW FROM d) = 3 THEN '30sec on / 30sec off intervals'
    ELSE 'Zone 2 cardio - conversational pace'
  END
FROM generate_series('2025-10-06'::date, '2025-12-28'::date, '1 day'::interval) d
WHERE EXTRACT(DOW FROM d) IN (1, 3, 5); -- Mon, Wed, Fri


-- ============================================================
-- PERSONAL RECORDS
-- ============================================================

INSERT INTO personal_records (athlete_id, exercise_id, record_type, value, achieved_date, previous_value, notes)
SELECT 
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  e.id, r.rtype, r.val, r.adate::date, r.prev, r.note
FROM exercises e
CROSS JOIN (VALUES
  ('barbell-bench-press', '1rm', 120.0, '2025-12-15', 115.0, 'Hit 265lb! Long time goal.'),
  ('barbell-bench-press', '5rm', 102.5, '2025-12-10', 100.0, 'Smooth 5x5 at 225lb'),
  ('barbell-back-squat', '1rm', 155.0, '2025-12-18', 150.0, '3 plate squat unlocked!'),
  ('barbell-back-squat', '5rm', 132.5, '2025-12-12', 127.5, 'Deep squats, no belt'),
  ('conventional-deadlift', '1rm', 195.0, '2025-12-20', 190.0, '4 plate pull. Felt like I had more.'),
  ('overhead-press', '1rm', 72.5, '2025-12-14', 70.0, 'Strict press, no leg drive'),
  ('barbell-row', '5rm', 92.5, '2025-12-11', 90.0, 'No body english, strict form'),
  ('pull-ups', 'max_reps_bodyweight', 18.0, '2025-11-28', 16.0, 'Dead hang, full ROM, chest to bar'),
  ('romanian-deadlift', '8rm', 110.0, '2025-12-16', 105.0, 'Smooth and controlled'),
  ('hip-thrust', '10rm', 130.0, '2025-12-19', 125.0, '2 sec pause at top each rep')
) AS r(slug, rtype, val, adate, prev, note)
WHERE e.slug = r.slug;


DO $$ BEGIN RAISE NOTICE '✅ Athlete data seeded! 72 workouts, 4000+ sets, 84 days of metrics/nutrition. Run 04_views_functions.sql next.'; END $$;
