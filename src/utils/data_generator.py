"""
Synthetic Data Generator for FitTrack Pro
==========================================

WHAT: Generates realistic multi-athlete fitness data for the FitTrack Pro
ETL pipeline, including body metrics, workout logs, nutrition, and an
exercise library.

WHY: A reproducible, seeded data generator lets us:
  1. Demo the full pipeline without real API access or user data
  2. Test edge cases (deload weeks, bodyweight exercises, cutting/bulking)
  3. Ensure deterministic output for snapshot-based regression tests

HOW: Six athlete profiles with distinct training styles drive templated
workout, nutrition, and body-metric generation over a 30-day window.
All randomness is seeded so identical inputs always produce identical outputs.

Usage:
    from src.utils.data_generator import FitTrackDataGenerator

    generator = FitTrackDataGenerator(seed=42, days=30)
    generator.generate_all("data/sample")
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# ============================================================
# Wger ID Mappings
# ============================================================
# These mirror the Wger REST API integer IDs so that our sample
# exercise JSON can be ingested by the same Bronze-layer code
# that handles live API responses.
# ============================================================

MUSCLE_IDS: dict[int, str] = {
    1: "biceps",
    2: "shoulders",
    3: "abs",
    4: "quads",
    5: "chest",
    6: "triceps",
    7: "lats",
    8: "back",
    9: "hamstrings",
    10: "glutes",
    11: "calves",
    12: "traps",
    13: "forearms",
    14: "obliques",
    15: "hip_flexors",
}

EQUIPMENT_IDS: dict[int, str] = {
    1: "barbell",
    3: "dumbbell",
    4: "machine",
    5: "bodyweight",
    6: "pull_up_bar",
    8: "cable",
    10: "kettlebell",
}

CATEGORY_IDS: dict[int, str] = {
    8: "Arms",
    9: "Legs",
    10: "Back/Abs",
    11: "Chest",
    12: "Back",
    13: "Shoulders",
    14: "Calves",
    15: "Glutes",
}


# ============================================================
# Exercise Library (Wger API JSON format)
# ============================================================

EXERCISE_LIBRARY: list[dict[str, Any]] = [
    {
        "id": 192,
        "name": "Barbell Bench Press",
        "description": "Lie flat on a bench and press a loaded barbell from chest level to full arm extension. Primarily targets the pectorals with secondary engagement of the shoulders and triceps.",
        "muscles": [5, 2, 6],
        "equipment": [1],
        "category": 11,
    },
    {
        "id": 289,
        "name": "Barbell Deadlift",
        "description": "Lift a loaded barbell from the floor to hip level by extending the hips and knees. A foundational posterior-chain movement that builds total-body strength.",
        "muscles": [10, 9, 8, 15],
        "equipment": [1],
        "category": 10,
    },
    {
        "id": 111,
        "name": "Barbell Squat",
        "description": "With a barbell across the upper back, descend by bending at the hips and knees until thighs are parallel, then drive upward. The primary compound movement for quadricep and glute development.",
        "muscles": [4, 10, 9],
        "equipment": [1],
        "category": 9,
    },
    {
        "id": 274,
        "name": "Overhead Press",
        "description": "Press a barbell from shoulder height to full lockout overhead while standing. Builds shoulder strength and stability with significant tricep and upper-trap involvement.",
        "muscles": [2, 6, 12],
        "equipment": [1],
        "category": 13,
    },
    {
        "id": 106,
        "name": "Barbell Row",
        "description": "Hinge at the hips and pull a barbell from arm's length to the lower ribcage. An essential horizontal pulling movement for back thickness and bicep development.",
        "muscles": [8, 7, 1],
        "equipment": [1],
        "category": 12,
    },
    {
        "id": 163,
        "name": "Incline Bench Press",
        "description": "Press a barbell or dumbbells on a bench set to 30-45 degrees. Shifts emphasis to the upper chest and anterior deltoids compared to the flat bench press.",
        "muscles": [5, 2, 6],
        "equipment": [1, 3],
        "category": 11,
    },
    {
        "id": 191,
        "name": "Front Squat",
        "description": "Squat with the barbell racked across the front deltoids in a clean grip. Demands greater core stability and quad engagement than the back squat.",
        "muscles": [4, 10, 3],
        "equipment": [1],
        "category": 9,
    },
    {
        "id": 405,
        "name": "Sumo Deadlift",
        "description": "Deadlift with a wide stance and hands inside the knees. Shifts emphasis toward the quads and adductors while reducing lower-back demand.",
        "muscles": [10, 9, 4],
        "equipment": [1],
        "category": 10,
    },
    {
        "id": 107,
        "name": "Pull-up",
        "description": "Hang from a bar with an overhand grip and pull the body upward until the chin clears the bar. A bodyweight staple for lat width and bicep strength.",
        "muscles": [7, 1, 8],
        "equipment": [6],
        "category": 12,
    },
    {
        "id": 82,
        "name": "Dip",
        "description": "Support the body on parallel bars and lower by bending the elbows, then press back up. Targets the chest, triceps, and anterior deltoids.",
        "muscles": [5, 6, 2],
        "equipment": [5],
        "category": 11,
    },
    {
        "id": 81,
        "name": "Dumbbell Curl",
        "description": "Curl a pair of dumbbells from arm's length to shoulder height while keeping the elbows stationary. An isolation movement for bicep peak and forearm development.",
        "muscles": [1],
        "equipment": [3],
        "category": 8,
    },
    {
        "id": 344,
        "name": "Tricep Pushdown",
        "description": "Push a cable attachment downward from chest height to full elbow extension. An isolation exercise for tricep mass and lockout strength.",
        "muscles": [6],
        "equipment": [8],
        "category": 8,
    },
    {
        "id": 148,
        "name": "Lateral Raise",
        "description": "Raise dumbbells out to the sides until arms are parallel with the floor. Isolates the lateral head of the deltoid for broader-looking shoulders.",
        "muscles": [2],
        "equipment": [3],
        "category": 13,
    },
    {
        "id": 115,
        "name": "Leg Press",
        "description": "Push a weighted sled away from the body using the legs while seated in a machine. A high-volume quad and glute builder that spares the lower back.",
        "muscles": [4, 10],
        "equipment": [4],
        "category": 9,
    },
    {
        "id": 351,
        "name": "Romanian Deadlift",
        "description": "Lower a barbell by hinging at the hips with a slight knee bend, feeling a deep hamstring stretch, then return to standing. Builds posterior-chain flexibility and strength.",
        "muscles": [9, 10],
        "equipment": [1],
        "category": 10,
    },
    {
        "id": 360,
        "name": "Face Pull",
        "description": "Pull a rope attachment on a cable machine toward the face with elbows high. Essential for rear-delt health, external rotation strength, and posture.",
        "muscles": [12, 2, 8],
        "equipment": [8],
        "category": 12,
    },
    {
        "id": 122,
        "name": "Cable Fly",
        "description": "Bring cable handles together in a wide arc at chest height, squeezing the pectorals at peak contraction. Provides constant tension throughout the range of motion.",
        "muscles": [5],
        "equipment": [8],
        "category": 11,
    },
    {
        "id": 116,
        "name": "Leg Curl",
        "description": "Curl a machine pad toward the glutes by flexing at the knee while lying prone. Isolates the hamstrings and calf muscles for balanced leg development.",
        "muscles": [9, 11],
        "equipment": [4],
        "category": 9,
    },
    {
        "id": 102,
        "name": "Calf Raise",
        "description": "Rise onto the balls of the feet against resistance, then lower under control. Targets the gastrocnemius and soleus for calf size and ankle stability.",
        "muscles": [11],
        "equipment": [4],
        "category": 14,
    },
    {
        "id": 88,
        "name": "Plank",
        "description": "Hold a rigid push-up position on the forearms for time, bracing the core throughout. Builds isometric abdominal and oblique endurance.",
        "muscles": [3, 14],
        "equipment": [5],
        "category": 10,
    },
]

# Quick lookup: exercise name -> exercise dict
_EXERCISE_BY_NAME: dict[str, dict[str, Any]] = {ex["name"]: ex for ex in EXERCISE_LIBRARY}


# ============================================================
# Athlete Profiles
# ============================================================

ATHLETES: list[dict[str, Any]] = [
    {
        "name": "Marcus Chen",
        "email": "marcus.chen@email.com",
        "style": "Powerlifter",
        "base_weight": 82.0,
        "weight_trend": 0.03,
        "body_fat_pct": 14.5,
        "resting_hr": 58,
        "training_days": [0, 2, 4, 5],  # Mon, Wed, Fri, Sat
        "calorie_target": 3200,
        "protein_per_kg": 2.2,
        "templates": {
            0: [  # Monday - Push
                ("Barbell Bench Press", 100, 5, True),
                ("Overhead Press", 60, 8, True),
                ("Dip", 20, 10, False),
            ],
            2: [  # Wednesday - Pull
                ("Barbell Deadlift", 160, 5, True),
                ("Barbell Row", 85, 8, True),
                ("Pull-up", 15, 8, False),
            ],
            4: [  # Friday - Legs
                ("Barbell Squat", 130, 5, True),
                ("Leg Press", 180, 10, False),
                ("Romanian Deadlift", 100, 8, True),
            ],
            5: [  # Saturday - Upper
                ("Incline Bench Press", 80, 8, True),
                ("Lateral Raise", 14, 12, False),
                ("Dumbbell Curl", 18, 12, False),
            ],
        },
    },
    {
        "name": "Priya Sharma",
        "email": "priya.sharma@email.com",
        "style": "CrossFit",
        "base_weight": 62.0,
        "weight_trend": 0.0,
        "body_fat_pct": 19.0,
        "resting_hr": 52,
        "training_days": [0, 1, 3, 4, 5],  # Mon, Tue, Thu, Fri, Sat
        "calorie_target": 2400,
        "protein_per_kg": 2.0,
        "templates": {
            0: [  # Monday
                ("Front Squat", 50, 8, True),
                ("Pull-up", 0, 8, False),
                ("Overhead Press", 35, 8, True),
            ],
            1: [  # Tuesday
                ("Barbell Deadlift", 80, 6, True),
                ("Dip", 0, 10, False),
                ("Barbell Bench Press", 45, 8, True),
            ],
            3: [  # Thursday
                ("Barbell Row", 50, 8, True),
                ("Barbell Squat", 55, 8, True),
                ("Plank", 0, 1, False),
            ],
            4: [  # Friday
                ("Front Squat", 50, 8, True),
                ("Face Pull", 15, 12, False),
                ("Overhead Press", 35, 8, True),
            ],
            5: [  # Saturday
                ("Barbell Deadlift", 80, 6, True),
                ("Pull-up", 0, 8, False),
                ("Barbell Bench Press", 45, 8, True),
            ],
        },
    },
    {
        "name": "James O'Brien",
        "email": "james.obrien@email.com",
        "style": "Bodybuilder",
        "base_weight": 95.0,
        "weight_trend": -0.04,
        "body_fat_pct": 18.0,
        "resting_hr": 62,
        "training_days": [0, 1, 2, 4, 5],  # Mon, Tue, Wed, Fri, Sat
        "calorie_target": 2800,
        "protein_per_kg": 2.5,
        "templates": {
            0: [  # Monday - Push
                ("Barbell Bench Press", 110, 8, True),
                ("Incline Bench Press", 85, 10, True),
                ("Cable Fly", 18, 12, False),
                ("Dip", 25, 10, False),
            ],
            1: [  # Tuesday - Pull
                ("Barbell Row", 90, 8, True),
                ("Pull-up", 20, 8, True),
                ("Face Pull", 20, 12, False),
                ("Dumbbell Curl", 16, 12, False),
            ],
            2: [  # Wednesday - Legs
                ("Barbell Squat", 140, 8, True),
                ("Leg Press", 200, 10, True),
                ("Leg Curl", 50, 12, False),
                ("Calf Raise", 80, 15, False),
            ],
            4: [  # Friday - Shoulders
                ("Overhead Press", 65, 8, True),
                ("Lateral Raise", 14, 15, False),
                ("Tricep Pushdown", 30, 12, False),
            ],
            5: [  # Saturday - Back 2
                ("Barbell Deadlift", 170, 5, True),
                ("Romanian Deadlift", 110, 8, True),
                ("Barbell Row", 85, 10, True),
            ],
        },
    },
    {
        "name": "Sofia Rodriguez",
        "email": "sofia.rodriguez@email.com",
        "style": "Strength",
        "base_weight": 68.0,
        "weight_trend": 0.0,
        "body_fat_pct": 20.0,
        "resting_hr": 55,
        "training_days": [0, 2, 4, 5],  # Mon, Wed, Fri, Sat
        "calorie_target": 2200,
        "protein_per_kg": 1.8,
        "templates": {
            0: [  # Monday - Squat day
                ("Barbell Squat", 90, 5, True),
                ("Front Squat", 65, 6, True),
                ("Leg Press", 140, 10, False),
            ],
            2: [  # Wednesday - Upper
                ("Barbell Bench Press", 60, 5, True),
                ("Overhead Press", 40, 8, True),
                ("Dip", 0, 10, False),
            ],
            4: [  # Friday - Pull
                ("Barbell Deadlift", 120, 5, True),
                ("Romanian Deadlift", 80, 8, True),
                ("Barbell Row", 55, 8, True),
            ],
            5: [  # Saturday - Full body
                ("Pull-up", 0, 6, False),
                ("Lateral Raise", 10, 12, False),
                ("Plank", 0, 1, False),
            ],
        },
    },
    {
        "name": "Tyler Washington",
        "email": "tyler.washington@email.com",
        "style": "Hybrid",
        "base_weight": 75.0,
        "weight_trend": 0.015,
        "body_fat_pct": 15.0,
        "resting_hr": 50,
        "training_days": [0, 2, 4],  # Mon, Wed, Fri
        "calorie_target": 2600,
        "protein_per_kg": 1.6,
        "templates": {
            0: [  # Monday - Full body 1
                ("Barbell Bench Press", 80, 8, True),
                ("Barbell Row", 70, 8, True),
                ("Dumbbell Curl", 14, 12, False),
            ],
            2: [  # Wednesday - Full body 2
                ("Barbell Squat", 100, 6, True),
                ("Barbell Deadlift", 120, 5, True),
                ("Calf Raise", 60, 15, False),
            ],
            4: [  # Friday - Full body 3
                ("Overhead Press", 50, 8, True),
                ("Pull-up", 10, 8, True),
                ("Tricep Pushdown", 25, 12, False),
            ],
        },
    },
    {
        "name": "Aiko Tanaka",
        "email": "aiko.tanaka@email.com",
        "style": "Calisthenics",
        "base_weight": 58.0,
        "weight_trend": 0.0,
        "body_fat_pct": 17.0,
        "resting_hr": 54,
        "training_days": [0, 1, 3, 4],  # Mon, Tue, Thu, Fri
        "calorie_target": 2000,
        "protein_per_kg": 1.8,
        "templates": {
            0: [  # Monday - Upper 1
                ("Pull-up", 0, 8, True),
                ("Dip", 0, 12, False),
                ("Plank", 0, 1, False),
            ],
            1: [  # Tuesday - Legs
                ("Barbell Squat", 45, 8, True),
                ("Calf Raise", 30, 15, False),
                ("Leg Curl", 25, 12, False),
            ],
            3: [  # Thursday - Upper 2
                ("Pull-up", 0, 8, True),
                ("Overhead Press", 25, 8, True),
                ("Face Pull", 10, 12, False),
            ],
            4: [  # Friday - Push
                ("Dip", 0, 12, False),
                ("Lateral Raise", 8, 12, False),
                ("Plank", 0, 1, False),
            ],
        },
    },
]


# ============================================================
# Workout Notes (comma-free for CSV safety)
# ============================================================

WORKOUT_NOTES: list[str] = [
    "Warm-up set",
    "Felt strong",
    "Good lockout",
    "Slight grind",
    "Fast and explosive",
    "Controlled tempo",
    "Good depth",
    "Last rep was tough",
    "Smooth reps",
    "Grip held well",
    "Back tight",
    "Good bracing",
    "Easy set",
    "Heavy but clean",
    "Paused rep",
    "Touch and go",
    "Belt on",
    "Speed work",
    "Technique focus",
    "Back-off set",
    "Solid set",
    "Full ROM",
    "Explosive concentric",
    "Slow eccentric",
]


# ============================================================
# Meal Templates
# ============================================================
# Format per entry: (food_name, base_calories, protein_g, carbs_g, fats_g, fiber_g)

MEAL_TEMPLATES: dict[str, list[tuple[str, int, int, int, int, int]]] = {
    "breakfast": [
        ("Scrambled Eggs with Toast", 520, 35, 40, 22, 3),
        ("Oatmeal with Berries and Whey", 480, 38, 55, 12, 7),
        ("Greek Yogurt Parfait", 420, 32, 45, 14, 4),
        ("Protein Pancakes", 550, 40, 60, 15, 3),
        ("Avocado Toast with Eggs", 490, 28, 38, 25, 6),
        ("Smoothie Bowl", 460, 30, 52, 16, 5),
    ],
    "lunch": [
        ("Grilled Chicken Rice Bowl", 680, 52, 70, 18, 6),
        ("Turkey Wrap with Veggies", 620, 45, 58, 20, 5),
        ("Chicken Caesar Salad", 580, 48, 25, 30, 4),
        ("Tuna Poke Bowl", 590, 42, 55, 22, 5),
        ("Beef Burrito Bowl", 720, 48, 65, 28, 8),
        ("Salmon Quinoa Bowl", 640, 44, 58, 24, 6),
    ],
    "dinner": [
        ("Salmon with Sweet Potatoes", 750, 48, 65, 28, 8),
        ("Steak with Broccoli and Rice", 820, 55, 60, 32, 6),
        ("Pasta with Meat Sauce", 720, 42, 80, 22, 5),
        ("Grilled Chicken with Roasted Vegetables", 650, 50, 45, 24, 7),
        ("Shrimp Stir-Fry with Noodles", 600, 38, 65, 18, 4),
        ("Lamb Chops with Couscous", 780, 52, 58, 30, 5),
    ],
    "snack": [
        ("Protein Shake with Banana", 350, 40, 35, 5, 2),
        ("Mixed Nuts and Dried Fruit", 280, 8, 25, 18, 3),
        ("Cottage Cheese with Berries", 220, 24, 18, 6, 2),
        ("Rice Cakes with Peanut Butter", 310, 12, 38, 14, 2),
        ("Protein Bar", 290, 30, 28, 8, 3),
        ("Hard-Boiled Eggs", 210, 18, 2, 14, 0),
    ],
    "pre_workout": [
        ("Pre-Workout Shake", 200, 15, 30, 2, 1),
        ("Rice Cakes with Honey", 250, 4, 55, 2, 0),
        ("Banana with Peanut Butter", 280, 8, 38, 14, 3),
    ],
    "post_workout": [
        ("Whey Protein Shake", 280, 45, 15, 4, 0),
        ("Recovery Smoothie", 350, 35, 40, 6, 3),
        ("Chocolate Milk with Protein", 320, 32, 38, 8, 1),
    ],
}


# ============================================================
# Generator Class
# ============================================================


class FitTrackDataGenerator:
    """
    Generates realistic multi-athlete fitness data for the FitTrack Pro
    ETL pipeline.

    All randomness is seeded so that the same seed + parameters always
    produce byte-identical output files.  This is critical for snapshot
    tests and repeatable demos.

    Args:
        seed: Random seed for reproducibility.
        days: Number of days of data to generate.
        start_date: First day of the generated data window (YYYY-MM-DD).
    """

    def __init__(
        self,
        seed: int = 42,
        days: int = 30,
        start_date: str = "2026-02-01",
    ) -> None:
        self.seed = seed
        self.days = days
        self.start_date = date.fromisoformat(start_date)
        self.athletes = ATHLETES
        self._rng = random.Random(seed)

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def generate_all(self, output_dir: str | Path) -> dict[str, int]:
        """
        Generate all sample datasets and write them to *output_dir*.

        Returns:
            Dictionary mapping filename -> row count for each generated file.
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        # Reset seed before each full generation run so that calling
        # generate_all twice yields identical data.
        self._rng = random.Random(self.seed)

        results: dict[str, int] = {}
        results["sample_body_metrics.csv"] = self.generate_body_metrics(out / "sample_body_metrics.csv")
        results["sample_workout_log.csv"] = self.generate_workouts(out / "sample_workout_log.csv")
        results["sample_nutrition.json"] = self.generate_nutrition(out / "sample_nutrition.json")
        results["sample_exercises.json"] = self.generate_exercises(out / "sample_exercises.json")
        return results

    # ----------------------------------------------------------
    # Body Metrics
    # ----------------------------------------------------------

    def generate_body_metrics(self, output_path: Path) -> int:
        """
        Generate daily body-composition and wellness metrics for every
        athlete over the configured date window.

        Returns:
            Total number of data rows written (excludes header).
        """
        rows: list[dict[str, Any]] = []
        for athlete in self.athletes:
            for day_idx in range(self.days):
                current_date = self.start_date + timedelta(days=day_idx)
                weekday = current_date.weekday()
                is_training = weekday in athlete["training_days"]

                weight = round(
                    athlete["base_weight"] + day_idx * athlete["weight_trend"] + self._rng.gauss(0, 0.3),
                    1,
                )

                body_fat = round(
                    athlete["body_fat_pct"] + self._rng.gauss(0, 0.2),
                    1,
                )
                body_fat = max(5.0, min(body_fat, 35.0))

                resting_hr = athlete["resting_hr"] + self._rng.randint(-3, 3)

                sleep_quality = self._rng.choice([5, 6, 6, 7, 7, 7, 8, 8, 8, 9])
                stress = self._rng.choice([1, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 7])
                recovery = int(70 + sleep_quality * 2 - stress * 1.5 + self._rng.gauss(0, 5))
                recovery = max(55, min(recovery, 98))

                steps = self._rng.randint(4000, 14000) + (2000 if is_training else 0)

                rows.append(
                    {
                        "date": current_date.isoformat(),
                        "athlete_email": athlete["email"],
                        "weight_kg": weight,
                        "body_fat_pct": body_fat,
                        "resting_heart_rate": resting_hr,
                        "sleep_quality": sleep_quality,
                        "stress_level": stress,
                        "recovery_score": recovery,
                        "steps": steps,
                    }
                )

        fieldnames = [
            "date",
            "athlete_email",
            "weight_kg",
            "body_fat_pct",
            "resting_heart_rate",
            "sleep_quality",
            "stress_level",
            "recovery_score",
            "steps",
        ]
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return len(rows)

    # ----------------------------------------------------------
    # Workouts
    # ----------------------------------------------------------

    def generate_workouts(self, output_path: Path) -> int:
        """
        Generate set-by-set workout logs for every athlete's training
        days, including progressive overload and a week-4 deload.

        Returns:
            Total number of set rows written (excludes header).
        """
        rows: list[dict[str, Any]] = []

        for athlete in self.athletes:
            templates = athlete["templates"]
            athlete_bodyweight = athlete["base_weight"]

            for day_idx in range(self.days):
                current_date = self.start_date + timedelta(days=day_idx)
                weekday = current_date.weekday()

                if weekday not in templates:
                    continue

                week_number = day_idx // 7  # 0-indexed week
                is_deload = 21 <= day_idx < 28  # Week 4 = deload

                # Progressive overload factor: +1-2.5% per week
                progression = 1.0 + week_number * self._rng.uniform(0.01, 0.025)

                exercises = templates[weekday]
                for exercise_name, base_weight, base_reps, is_compound in exercises:
                    # Determine working weight for this week
                    working_weight = round(base_weight * progression, 1)
                    if is_deload:
                        working_weight = round(base_weight * 0.8, 1)

                    # Bodyweight exercises: use athlete's current approximate weight
                    is_bodyweight = base_weight == 0
                    current_bw = round(
                        athlete_bodyweight + day_idx * athlete["weight_trend"],
                        1,
                    )

                    num_sets = 3 if (is_deload or not is_compound) else 4

                    for set_num in range(1, num_sets + 1):
                        if is_bodyweight:
                            set_weight = current_bw
                        elif set_num == 1:
                            # Warm-up / ramp set: 85% of working weight
                            set_weight = round(working_weight * 0.85, 1)
                        else:
                            set_weight = working_weight

                        # Reps vary by set position
                        if set_num == 1:
                            set_reps = base_reps + 2
                        elif set_num == num_sets and num_sets == 4:
                            set_reps = max(1, base_reps - 1)
                        else:
                            set_reps = base_reps

                        # Add small rep variance (+/- 1 occasionally)
                        if self._rng.random() < 0.2:
                            set_reps += self._rng.choice([-1, 1])
                        set_reps = max(1, set_reps)

                        # RPE scales with set difficulty
                        if set_num == 1:
                            rpe = round(self._rng.uniform(6.5, 7.5), 1)
                        elif set_num == num_sets and num_sets == 4:
                            rpe = round(self._rng.uniform(8.5, 9.0), 1)
                        else:
                            rpe = round(self._rng.uniform(7.5, 8.5), 1)

                        if is_deload:
                            rpe = round(rpe - 1.5, 1)
                            rpe = max(5.0, rpe)

                        # Select a contextually reasonable note
                        if set_num == 1 and self._rng.random() < 0.4:
                            note = "Warm-up set"
                        elif set_num == num_sets and rpe >= 8.5:
                            note = self._rng.choice(
                                [
                                    "Last rep was tough",
                                    "Slight grind",
                                    "Heavy but clean",
                                    "Belt on",
                                ]
                            )
                        elif is_deload:
                            note = self._rng.choice(
                                [
                                    "Easy set",
                                    "Speed work",
                                    "Technique focus",
                                    "Back-off set",
                                    "Controlled tempo",
                                ]
                            )
                        else:
                            note = self._rng.choice(WORKOUT_NOTES)

                        rows.append(
                            {
                                "date": current_date.isoformat(),
                                "athlete_email": athlete["email"],
                                "exercise": exercise_name,
                                "set_number": set_num,
                                "weight": set_weight,
                                "weight_unit": "kg",
                                "reps": set_reps,
                                "rpe": rpe,
                                "notes": note,
                            }
                        )

        fieldnames = [
            "date",
            "athlete_email",
            "exercise",
            "set_number",
            "weight",
            "weight_unit",
            "reps",
            "rpe",
            "notes",
        ]
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return len(rows)

    # ----------------------------------------------------------
    # Nutrition
    # ----------------------------------------------------------

    def generate_nutrition(self, output_path: Path) -> int:
        """
        Generate per-meal nutrition logs for every athlete, scaling
        macros toward each athlete's daily calorie and protein targets.

        Returns:
            Total number of meal entries written.
        """
        all_meals: list[dict[str, Any]] = []

        for athlete in self.athletes:
            calorie_target = athlete["calorie_target"]
            protein_target = round(athlete["protein_per_kg"] * athlete["base_weight"])

            for day_idx in range(self.days):
                current_date = self.start_date + timedelta(days=day_idx)
                weekday = current_date.weekday()
                is_training = weekday in athlete["training_days"]

                day_factor = 1.1 if is_training else 0.9

                # Core meals everyone eats
                meal_types = ["breakfast", "lunch", "dinner", "snack"]
                if is_training:
                    meal_types.append("pre_workout")
                    meal_types.append("post_workout")

                # Compute a rough scaling factor to approach the calorie target.
                # Sum the base calories of one random pick per meal type, then
                # scale all meals uniformly.
                base_picks: list[tuple[str, tuple[str, int, int, int, int, int]]] = []
                for mt in meal_types:
                    template = self._rng.choice(MEAL_TEMPLATES[mt])
                    base_picks.append((mt, template))

                raw_cals = sum(t[1] for _, t in base_picks)
                target_cals = calorie_target * day_factor
                if raw_cals > 0:
                    cal_scale = target_cals / raw_cals
                else:
                    cal_scale = 1.0

                # Similarly nudge protein toward the target
                raw_protein = sum(t[2] for _, t in base_picks)
                if raw_protein > 0:
                    pro_scale = (protein_target * day_factor) / raw_protein
                else:
                    pro_scale = 1.0

                # Blend calorie and protein scaling so neither is wildly off.
                # Lean toward calorie accuracy (60/40 split).
                blend = 0.6 * cal_scale + 0.4 * pro_scale

                for meal_type, template in base_picks:
                    food_name, base_cal, base_pro, base_carb, base_fat, base_fiber = template

                    # Apply scaling with a small per-meal variance
                    meal_var = self._rng.uniform(0.92, 1.08)
                    factor = blend * meal_var

                    calories = round(base_cal * factor)
                    protein_g = round(base_pro * factor, 1)
                    carbs_g = round(base_carb * factor, 1)
                    fats_g = round(base_fat * factor, 1)
                    fiber_g = round(base_fiber * factor, 1)
                    water_ml = self._rng.randint(400, 700)

                    all_meals.append(
                        {
                            "athlete_email": athlete["email"],
                            "log_date": current_date.isoformat(),
                            "meal_type": meal_type,
                            "food_name": food_name,
                            "calories": calories,
                            "protein_g": protein_g,
                            "carbs_g": carbs_g,
                            "fats_g": fats_g,
                            "fiber_g": fiber_g,
                            "water_ml": water_ml,
                        }
                    )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_meals, f, indent=2, ensure_ascii=False)

        return len(all_meals)

    # ----------------------------------------------------------
    # Exercise Library
    # ----------------------------------------------------------

    def generate_exercises(self, output_path: Path) -> int:
        """
        Write the 20-exercise library in Wger API JSON format.

        Returns:
            Number of exercises written.
        """
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(EXERCISE_LIBRARY, f, indent=2, ensure_ascii=False)

        return len(EXERCISE_LIBRARY)


# ============================================================
# CLI Entry Point
# ============================================================

if __name__ == "__main__":
    gen = FitTrackDataGenerator()
    results = gen.generate_all("data/sample")
    print("FitTrack Pro - Synthetic Data Generator")
    print("=" * 42)
    for fname, count in results.items():
        print(f"  {fname}: {count} rows")
    print(f"\nTotal records: {sum(results.values())}")
