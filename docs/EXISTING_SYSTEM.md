# Existing System Documentation

## What Was Already Built

Before the ETL pipeline, this project contained a complete **FitTrack Pro** fitness database designed for Supabase (PostgreSQL). Here's what each file does and how it connects to the new pipeline system.

---

## File Inventory

### 01_schema.sql — Database Schema

**What it creates:** 12 tables with proper relationships, custom ENUM types, indexes, and computed columns.

**Tables:**

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `athletes` | Core user profiles | email, full_name, height_cm, activity_level |
| `exercises` | Master exercise library (60+) | name, slug, primary_muscle, equipment, difficulty |
| `workout_programs` | Training program definitions | goal, split, duration_weeks, days_per_week |
| `workouts` | Individual training sessions | workout_date, status, total_volume_kg, energy_level |
| `workout_sets` | Individual sets (the core data) | weight_kg, reps, rpe, set_type |
| `personal_records` | PR tracking per exercise | record_type (1rm, 5rm, etc.), value |
| `body_metrics` | Daily body composition | weight_kg, body_fat_pct, measurements |
| `nutrition_logs` | Meal tracking | calories, protein_g, carbs_g, fats_g |
| `cardio_sessions` | Cardio tracking | cardio_type, duration, heart_rate |
| `supplement_log` | Supplement intake | supplement_name, dosage, timing |
| `workout_tags` | Flexible tagging system | name, color |
| `workout_tag_assignments` | Tags linked to workouts | workout_id, tag_id |

**Custom ENUM types (12):** muscle_group, exercise_type, equipment_type, difficulty_level, program_goal, split_type, set_type, workout_status, meal_type, cardio_type

**Key design decisions:**
- UUID primary keys everywhere (standard for distributed systems)
- `GENERATED ALWAYS AS` computed column for workout duration
- PostgreSQL arrays for secondary_muscles and tips
- Strategic indexes on foreign keys and common query patterns

### 02_seed_exercises.sql — Exercise Library

Seeds 60+ exercises across all muscle groups: chest (8), back (8), shoulders (6), arms (8), legs (13), core (5), olympic/power (3), bodyweight (4).

Each exercise includes: coaching cues (tips array), equipment needed, difficulty level, and whether it's unilateral.

### 03_seed_athlete_data.sql — 12 Weeks of Training Data

Creates a complete athlete profile (Marcus Chen) with 12 weeks of Push/Pull/Legs training data:
- **72 workouts** (6 days/week x 12 weeks)
- **4,000+ workout sets** with progressive overload
- **84 days** of body metrics (daily weigh-ins, weekly measurements)
- **504 nutrition logs** (6 meals/day)
- **504 supplement entries** (daily stack)
- **36 cardio sessions** (3x/week)
- **10 personal records** across key lifts

The data simulates realistic progressive overload: weights increase ~2.5% every 3 weeks with week 12 as a deload.

### 04_views_functions.sql — Analytics Layer

**Views (5):**
- `weekly_training_summary` — Weekly volume, sets, and workout counts
- `exercise_progression` — Weight progression per exercise over time
- `daily_nutrition_totals` — Daily macro totals and water intake
- `body_composition_weekly` — Weekly body composition snapshots
- `muscle_volume_distribution` — Volume per muscle group per week

**Functions (6):**
- `calculate_e1rm()` — Estimated one-rep max (Epley formula)
- `get_workout_detail()` — Full workout breakdown
- `get_training_stats()` — Dashboard stats as JSON
- `get_e1rm_history()` — E1RM trends over time
- `search_exercises()` — Full-text search on exercise library
- `log_set()` — Log a set with auto-PR detection

### 05_rls_policies.sql — Row Level Security

Full RLS policy coverage on every table. Each athlete can only see their own data. Exercises are publicly readable but only creators can edit custom ones.

---

## How the Existing System Connects to the ETL Pipeline

The existing SQL schema is the **Gold layer target**. The ETL pipeline ingests raw data from external sources (Wger API, USDA API, file uploads) and transforms it to land in these existing tables.

```
External Sources          ETL Pipeline              Existing Schema
+---------------+    +------------------+    +-------------------+
| Wger API      | -> | Bronze (raw)     | -> | exercises         |
| USDA API      | -> | Silver (clean)   | -> | nutrition_logs    |
| File Uploads  | -> | Gold (business)  | -> | workouts          |
|               |    |                  | -> | body_metrics      |
+---------------+    +------------------+    +-------------------+
```

The pipeline EXTENDS the existing schema with:
- `06_staging_tables.sql` — Staging areas for ETL data
- `07_etl_procedures.sql` — SQL procedures for transformations
