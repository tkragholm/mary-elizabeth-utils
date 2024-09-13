import os

# Base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Data preparation
START_YEAR = 2000
END_YEAR = 2022

# Register names
REGISTERS = ["LPR", "MFR", "IND", "BEF", "UDDF"]

# Table names
TABLE_NAMES = [
    "Person_Year_Income",
    "Person",
    "Family",
    "Child",
    "Diagnosis",
    "Employment",
    "Education",
    "Healthcare",
    "Time",
    "Socioeconomic_Status",
    "Treatment_Period",
    "Person_Child",
    "Person_Family",
]

# Severe chronic disease codes
SEVERE_CHRONIC_CODES = [
    "Q20",
    "Q21",
    "Q22",
    "Q23",
    "Q24",
    "Q25",
    "Q26",
    "Q27",
    "Q28",
    "E10",
    "C00",
    "C97",
    "G40",
    "G80",
]

# Analysis variables
NUMERIC_COLS = ["birth_date", "family_size", "total_income"]
CATEGORICAL_COLS = ["gender", "origin_type", "family_type", "socioeconomic_status"]
