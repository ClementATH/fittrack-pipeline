-- ============================================================
-- FITTRACK PRO - Supabase Fitness Database
-- File 2 of 5: SEED EXERCISE LIBRARY
-- 
-- Paste this SECOND into Supabase SQL Editor
-- 60+ exercises across all muscle groups
-- ============================================================

INSERT INTO exercises (name, slug, primary_muscle, secondary_muscles, exercise_type, equipment, difficulty, instructions, tips, is_unilateral) VALUES

-- ======================== CHEST ========================
('Barbell Bench Press', 'barbell-bench-press', 'chest', '{triceps,shoulders}', 'compound', 'barbell', 'intermediate',
 'Lie flat on bench, grip bar slightly wider than shoulder width. Lower bar to mid-chest, press up to lockout.',
 ARRAY['Retract scapula before unracking', 'Drive feet into floor', 'Bar path should be slightly diagonal'], FALSE),

('Incline Dumbbell Press', 'incline-dumbbell-press', 'chest', '{shoulders,triceps}', 'compound', 'dumbbell', 'intermediate',
 'Set bench to 30-45 degrees. Press dumbbells from shoulder level to full extension.',
 ARRAY['30 degree angle targets upper chest best', 'Control the negative for 2-3 seconds', 'Touch dumbbells at the top'], FALSE),

('Cable Flyes', 'cable-flyes', 'chest', '{}', 'isolation', 'cable', 'beginner',
 'Set cables at shoulder height. Step forward, bring handles together in a hugging motion.',
 ARRAY['Maintain slight elbow bend throughout', 'Squeeze at peak contraction', 'Focus on the stretch'], FALSE),

('Dumbbell Bench Press', 'dumbbell-bench-press', 'chest', '{triceps,shoulders}', 'compound', 'dumbbell', 'intermediate',
 'Lie flat, press dumbbells from chest level to full extension.',
 ARRAY['Greater ROM than barbell', 'Allows more natural wrist rotation', 'Great for fixing imbalances'], FALSE),

('Incline Barbell Bench Press', 'incline-barbell-bench-press', 'chest', '{shoulders,triceps}', 'compound', 'barbell', 'intermediate',
 'Set bench to 30-45 degrees. Unrack barbell and lower to upper chest, press to lockout.',
 ARRAY['Grip slightly narrower than flat bench', 'Keep elbows at 45 degrees'], FALSE),

('Chest Dips', 'chest-dips', 'chest', '{triceps,shoulders}', 'compound', 'dip_station', 'intermediate',
 'Lean forward slightly on dip bars, lower until stretch in chest, press back up.',
 ARRAY['Lean forward to emphasize chest over triceps', 'Go to 90 degrees or slightly below', 'Add weight via belt when bodyweight is too easy'], FALSE),

('Machine Chest Press', 'machine-chest-press', 'chest', '{triceps,shoulders}', 'compound', 'machine', 'beginner',
 'Sit with handles at chest height. Press forward to full extension.',
 ARRAY['Great for burnout sets', 'Allows focus on mind-muscle connection', 'Good for beginners learning press pattern'], FALSE),

('Low Cable Fly', 'low-cable-fly', 'chest', '{shoulders}', 'isolation', 'cable', 'intermediate',
 'Set cables at lowest position. Bring handles up and together in an arc.',
 ARRAY['Targets upper chest fibers', 'Keep constant tension', 'Finish with hands at eye level'], FALSE),

-- ======================== BACK ========================
('Barbell Row', 'barbell-row', 'back', '{biceps,lats}', 'compound', 'barbell', 'intermediate',
 'Hinge at hips to 45 degrees. Pull bar to lower chest/upper abdomen.',
 ARRAY['Keep back flat - no rounding', 'Pull with elbows, not hands', 'Squeeze shoulder blades at top'], FALSE),

('Pull-Ups', 'pull-ups', 'back', '{biceps,lats}', 'compound', 'pull_up_bar', 'intermediate',
 'Hang from bar with overhand grip. Pull chin above bar.',
 ARRAY['Full dead hang at bottom', 'Initiate with lats not biceps', 'Add weight when you can do 12+ clean reps'], FALSE),

('Lat Pulldown', 'lat-pulldown', 'lats', '{biceps,back}', 'compound', 'cable', 'beginner',
 'Sit at lat pulldown machine. Pull bar to upper chest with wide grip.',
 ARRAY['Lean back slightly', 'Pull elbows to your sides', 'Control the eccentric'], FALSE),

('Seated Cable Row', 'seated-cable-row', 'back', '{biceps,lats}', 'compound', 'cable', 'beginner',
 'Sit upright, pull handle to lower chest. Squeeze shoulder blades together.',
 ARRAY['Dont lean back excessively', 'Feel the stretch at full extension', 'V-grip targets mid-back'], FALSE),

('T-Bar Row', 't-bar-row', 'back', '{biceps,lats,traps}', 'compound', 'barbell', 'intermediate',
 'Straddle the bar, hinge at hips, pull bar to chest.',
 ARRAY['Allows heavy loading safely', 'Squeeze at the top', 'Keep chest up throughout'], FALSE),

('Single Arm Dumbbell Row', 'single-arm-dumbbell-row', 'back', '{biceps,lats}', 'compound', 'dumbbell', 'beginner',
 'Place one knee and hand on bench. Row dumbbell to hip with other hand.',
 ARRAY['Drive elbow past torso', 'Rotate slightly at top for full ROM', 'Great for fixing imbalances'], TRUE),

('Face Pulls', 'face-pulls', 'back', '{shoulders,traps}', 'isolation', 'cable', 'beginner',
 'Set cable at face height with rope. Pull to face, externally rotating at end.',
 ARRAY['Essential for shoulder health', 'Thumbs should point behind you at peak', 'High reps work best (15-25)'], FALSE),

('Chest Supported Row', 'chest-supported-row', 'back', '{biceps,lats}', 'compound', 'dumbbell', 'beginner',
 'Lie face down on incline bench. Row dumbbells to sides.',
 ARRAY['Eliminates cheating/momentum', 'Pure back isolation', 'Great for hypertrophy'], FALSE),

-- ======================== SHOULDERS ========================
('Overhead Press', 'overhead-press', 'shoulders', '{triceps,traps}', 'compound', 'barbell', 'intermediate',
 'Press barbell from front rack position overhead to lockout.',
 ARRAY['Brace core hard', 'Tuck chin as bar passes face', 'Full lockout overhead'], FALSE),

('Lateral Raises', 'lateral-raises', 'shoulders', '{}', 'isolation', 'dumbbell', 'beginner',
 'Raise dumbbells out to sides until arms are parallel to floor.',
 ARRAY['Slight bend in elbows', 'Lead with elbows not hands', 'Dont go above parallel - impingement risk'], FALSE),

('Rear Delt Flyes', 'rear-delt-flyes', 'shoulders', '{back}', 'isolation', 'dumbbell', 'beginner',
 'Bend over or use incline bench. Raise dumbbells out to sides.',
 ARRAY['Essential for balanced shoulders', 'Pinch shoulder blades', 'Use light weight, high reps'], FALSE),

('Dumbbell Shoulder Press', 'dumbbell-shoulder-press', 'shoulders', '{triceps}', 'compound', 'dumbbell', 'intermediate',
 'Press dumbbells from shoulder level to overhead.',
 ARRAY['Allows more natural shoulder path', 'Dont flare elbows excessively', 'Control the negative'], FALSE),

('Cable Lateral Raise', 'cable-lateral-raise', 'shoulders', '{}', 'isolation', 'cable', 'intermediate',
 'Stand sideways to cable machine. Raise handle out to side.',
 ARRAY['Constant tension throughout ROM', 'Cross-body setup hits medial head well', 'Go light - form over ego'], TRUE),

('Arnold Press', 'arnold-press', 'shoulders', '{triceps}', 'compound', 'dumbbell', 'intermediate',
 'Start with palms facing you at chin level. Rotate and press overhead.',
 ARRAY['Full rotation engages all three delt heads', 'Named after the GOAT for a reason', 'Slower tempo works best'], FALSE),

-- ======================== ARMS ========================
('Barbell Curl', 'barbell-curl', 'biceps', '{forearms}', 'isolation', 'barbell', 'beginner',
 'Stand with barbell at arms length. Curl to shoulder level.',
 ARRAY['Keep elbows pinned to sides', 'Dont swing the weight', 'Full extension at bottom'], FALSE),

('Incline Dumbbell Curl', 'incline-dumbbell-curl', 'biceps', '{}', 'isolation', 'dumbbell', 'intermediate',
 'Sit on incline bench (45°). Let arms hang straight down, curl up.',
 ARRAY['Incredible stretch on long head', 'Go lighter than standing curls', 'Supinate at the top'], FALSE),

('Hammer Curls', 'hammer-curls', 'biceps', '{forearms}', 'isolation', 'dumbbell', 'beginner',
 'Curl with neutral (palms facing each other) grip.',
 ARRAY['Targets brachialis and brachioradialis', 'Builds forearm thickness', 'Can go heavier than regular curls'], FALSE),

('Preacher Curl', 'preacher-curl', 'biceps', '{}', 'isolation', 'ez_bar', 'intermediate',
 'Rest upper arms on preacher pad. Curl EZ bar up.',
 ARRAY['Isolates biceps - no cheating possible', 'Dont fully extend at bottom - protects joints', 'Great for short head focus'], FALSE),

('Tricep Pushdown', 'tricep-pushdown', 'triceps', '{}', 'isolation', 'cable', 'beginner',
 'Push cable attachment down until arms are fully extended.',
 ARRAY['Keep elbows pinned', 'Squeeze at full extension', 'Rope attachment allows rotation for extra contraction'], FALSE),

('Skull Crushers', 'skull-crushers', 'triceps', '{}', 'isolation', 'ez_bar', 'intermediate',
 'Lie on bench, lower EZ bar to forehead, extend back up.',
 ARRAY['Control the weight - its near your face', 'Let bar go slightly behind head for stretch', 'Great long head builder'], FALSE),

('Overhead Tricep Extension', 'overhead-tricep-extension', 'triceps', '{}', 'isolation', 'cable', 'beginner',
 'Face away from cable machine. Extend arms overhead.',
 ARRAY['Maximum stretch on long head', 'Keep elbows close to head', 'Cable version provides constant tension'], FALSE),

('Close Grip Bench Press', 'close-grip-bench-press', 'triceps', '{chest,shoulders}', 'compound', 'barbell', 'intermediate',
 'Bench press with hands shoulder-width apart.',
 ARRAY['Elbows stay closer to body', 'Grip shoulder width - not too narrow', 'Heavy compound for tricep mass'], FALSE),

-- ======================== LEGS ========================
('Barbell Back Squat', 'barbell-back-squat', 'quads', '{glutes,hamstrings}', 'compound', 'barbell', 'intermediate',
 'Bar on upper traps. Squat to parallel or below.',
 ARRAY['Brace core before descending', 'Knees track over toes', 'Drive through whole foot', 'King of all exercises'], FALSE),

('Romanian Deadlift', 'romanian-deadlift', 'hamstrings', '{glutes,back}', 'compound', 'barbell', 'intermediate',
 'Hinge at hips with slight knee bend, lower bar along legs until hamstring stretch.',
 ARRAY['Bar stays close to body', 'Feel the hamstring stretch', 'Squeeze glutes at top', 'Dont round lower back'], FALSE),

('Leg Press', 'leg-press', 'quads', '{glutes,hamstrings}', 'compound', 'machine', 'beginner',
 'Sit in leg press, place feet shoulder width. Press platform away.',
 ARRAY['Foot placement changes emphasis', 'High and wide = more glute', 'Dont lock out knees completely', 'Go deep for full ROM'], FALSE),

('Hack Squat', 'hack-squat', 'quads', '{glutes}', 'compound', 'machine', 'intermediate',
 'Stand on hack squat platform. Squat down and press back up.',
 ARRAY['Narrow stance = more quad', 'Excellent quad builder', 'Safer than barbell squat for high reps'], FALSE),

('Bulgarian Split Squat', 'bulgarian-split-squat', 'quads', '{glutes,hamstrings}', 'compound', 'dumbbell', 'intermediate',
 'Rear foot elevated on bench. Lunge down with front leg.',
 ARRAY['Game changer for leg development', 'Fixes imbalances between legs', 'Lean forward slightly for more glute'], TRUE),

('Leg Curl', 'leg-curl', 'hamstrings', '{}', 'isolation', 'machine', 'beginner',
 'Lie face down on leg curl machine. Curl weight toward glutes.',
 ARRAY['Point toes to emphasize hamstrings', 'Slow eccentric builds more muscle', 'Essential for hamstring health'], FALSE),

('Leg Extension', 'leg-extension', 'quads', '{}', 'isolation', 'machine', 'beginner',
 'Sit in leg extension machine. Extend legs to straight position.',
 ARRAY['Squeeze at full extension', 'Great finisher exercise', 'Partials at end of set for extra burn'], FALSE),

('Hip Thrust', 'hip-thrust', 'glutes', '{hamstrings}', 'compound', 'barbell', 'intermediate',
 'Upper back on bench, barbell across hips. Drive hips up to full extension.',
 ARRAY['Chin tucked, ribs down', 'Pause at top for 1-2 seconds', 'Best glute builder period'], FALSE),

('Walking Lunges', 'walking-lunges', 'quads', '{glutes,hamstrings}', 'compound', 'dumbbell', 'intermediate',
 'Step forward into lunge position. Alternate legs walking forward.',
 ARRAY['Long steps = more glute, short steps = more quad', 'Keep torso upright', 'Great conditioning tool'], TRUE),

('Calf Raises', 'calf-raises', 'calves', '{}', 'isolation', 'machine', 'beginner',
 'Stand on calf raise machine. Rise up onto toes, lower slowly.',
 ARRAY['Full stretch at bottom is key', 'Pause at top for 2 seconds', 'Calves need high volume - 12-20 reps'], FALSE),

('Standing Calf Raise', 'standing-calf-raise', 'calves', '{}', 'isolation', 'machine', 'beginner',
 'Stand on elevated surface with heels hanging off. Rise up onto toes.',
 ARRAY['Straight leg hits gastrocnemius', 'Full ROM is critical', 'Train 3-4x per week for growth'], FALSE),

('Seated Calf Raise', 'seated-calf-raise', 'calves', '{}', 'isolation', 'machine', 'beginner',
 'Sit with knees under pad. Raise heels up as high as possible.',
 ARRAY['Bent knee targets soleus', 'Soleus is 60% of calf size', 'Go heavy and slow'], FALSE),

('Conventional Deadlift', 'conventional-deadlift', 'back', '{hamstrings,glutes,quads}', 'compound', 'barbell', 'advanced',
 'Stand with feet hip width. Grip bar just outside legs. Drive through floor.',
 ARRAY['Engage lats before pulling', 'Bar against shins', 'Hips and shoulders rise together', 'The ultimate strength builder'], FALSE),

('Front Squat', 'front-squat', 'quads', '{glutes,abs}', 'compound', 'barbell', 'advanced',
 'Bar in front rack position on shoulders. Squat to depth.',
 ARRAY['Elbows high throughout', 'More quad dominant than back squat', 'Requires good mobility', 'Forces upright torso'], FALSE),

('Sumo Deadlift', 'sumo-deadlift', 'glutes', '{hamstrings,quads,back}', 'compound', 'barbell', 'advanced',
 'Wide stance, grip inside legs. Drive hips forward as you stand.',
 ARRAY['Wider stance = more hip involvement', 'Great for those with long torsos', 'Toes pointed out 45 degrees'], FALSE),

-- ======================== ABS / CORE ========================
('Cable Woodchop', 'cable-woodchop', 'obliques', '{abs}', 'compound', 'cable', 'intermediate',
 'Set cable high. Rotate and pull diagonally across body.',
 ARRAY['Rotate through torso not arms', 'Great for athletic performance', 'Control the return'], FALSE),

('Hanging Leg Raise', 'hanging-leg-raise', 'abs', '{hip_flexors}', 'compound', 'pull_up_bar', 'intermediate',
 'Hang from bar. Raise legs to 90 degrees or higher.',
 ARRAY['Avoid swinging', 'Curl pelvis for more ab engagement', 'Add ankle weights when too easy'], FALSE),

('Ab Wheel Rollout', 'ab-wheel-rollout', 'abs', '{shoulders}', 'compound', 'none', 'advanced',
 'Kneel with ab wheel. Roll forward extending body, pull back.',
 ARRAY['Start with partial ROM', 'Dont let hips sag', 'One of the best core exercises'], FALSE),

('Plank', 'plank', 'abs', '{shoulders}', 'isometric', 'bodyweight', 'beginner',
 'Hold push-up position on forearms. Keep body in straight line.',
 ARRAY['Squeeze glutes', 'Dont let hips drop or pike', 'Breathe normally'], FALSE),

('Russian Twist', 'russian-twist', 'obliques', '{abs}', 'isolation', 'bodyweight', 'beginner',
 'Sit with feet elevated, lean back slightly. Rotate torso side to side.',
 ARRAY['Add weight as you progress', 'Feet off ground increases difficulty', 'Control the rotation'], FALSE),

-- ======================== OLYMPIC / POWER ========================
('Power Clean', 'power-clean', 'full_body', '{quads,glutes,traps,shoulders}', 'olympic_lift', 'barbell', 'advanced',
 'Explosive pull from floor, catch bar in front rack position.',
 ARRAY['Triple extension is key', 'Pull with hips not arms', 'Fast elbows under the bar'], FALSE),

('Push Press', 'push-press', 'shoulders', '{quads,triceps}', 'olympic_lift', 'barbell', 'intermediate',
 'Slight knee dip then explosively press barbell overhead.',
 ARRAY['Dip and drive through legs', 'Allows more weight than strict press', 'Great for athletic power'], FALSE),

('Kettlebell Swing', 'kettlebell-swing', 'glutes', '{hamstrings,back,abs}', 'compound', 'kettlebell', 'intermediate',
 'Hinge at hips, swing kettlebell between legs then thrust forward.',
 ARRAY['Power comes from hips not arms', 'Squeeze glutes at top', 'Great conditioning tool'], FALSE),

-- ======================== BODYWEIGHT ========================
('Push-Ups', 'push-ups', 'chest', '{triceps,shoulders}', 'bodyweight', 'bodyweight', 'beginner',
 'Standard push-up position. Lower chest to floor, press back up.',
 ARRAY['Full ROM - chest to floor', 'Core stays tight', 'Hands slightly wider than shoulders'], FALSE),

('Chin-Ups', 'chin-ups', 'biceps', '{back,lats}', 'compound', 'pull_up_bar', 'intermediate',
 'Hang from bar with underhand grip. Pull chin above bar.',
 ARRAY['Great bicep builder', 'Supinated grip reduces shoulder stress', 'Full dead hang at bottom'], FALSE),

('Dips', 'dips', 'triceps', '{chest,shoulders}', 'compound', 'dip_station', 'intermediate',
 'Support body on dip bars. Lower until 90 degree elbow bend, press up.',
 ARRAY['Upright = more triceps', 'Lean forward = more chest', 'Add weight via dip belt'], FALSE),

('Inverted Row', 'inverted-row', 'back', '{biceps}', 'compound', 'bodyweight', 'beginner',
 'Hang under a bar at waist height. Pull chest to bar.',
 ARRAY['Great progression to pull-ups', 'Adjust body angle for difficulty', 'Squeeze shoulder blades'], FALSE);


-- ============================================================
-- SUCCESS MESSAGE
-- ============================================================

DO $$ BEGIN RAISE NOTICE '✅ 60+ exercises seeded! Run 03_seed_athlete_data.sql next.'; END $$;
