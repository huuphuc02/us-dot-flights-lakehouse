"""
Data Schema Definitions for US DOT Flights Data
Defines the structure and data types for flight data columns
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# Flight data schema based on US DOT BTS On-Time Performance data
FLIGHT_DATA_SCHEMA = StructType([
    StructField("FL_DATE", TimestampType(), True),  # Flight date
    StructField("OP_UNIQUE_CARRIER", StringType(), True),  # Operating carrier code
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),  # Flight number
    StructField("ORIGIN_AIRPORT_ID", IntegerType(), True),  # Origin airport ID
    StructField("ORIGIN", StringType(), True),  # Origin airport code
    StructField("DEST_AIRPORT_ID", IntegerType(), True),  # Destination airport ID
    StructField("DEST", StringType(), True),  # Destination airport code
    StructField("CRS_DEP_TIME", StringType(), True),  # Scheduled departure time (HHMM format)
    StructField("DEP_TIME", StringType(), True),  # Actual departure time (HHMM format)
    StructField("DEP_DELAY", DoubleType(), True),  # Departure delay in minutes
    StructField("DEP_DELAY_NEW", DoubleType(), True),  # New departure delay field
    StructField("CRS_ARR_TIME", StringType(), True),  # Scheduled arrival time (HHMM format)
    StructField("ARR_TIME", StringType(), True),  # Actual arrival time (HHMM format)
    StructField("ARR_DELAY", DoubleType(), True),  # Arrival delay in minutes
    StructField("ARR_DELAY_NEW", DoubleType(), True),  # New arrival delay field
    StructField("CANCELLED", DoubleType(), True),  # Cancellation flag (0=No, 1=Yes)
    StructField("DIVERTED", DoubleType(), True),  # Diversion flag (0=No, 1=Yes)
    StructField("AIR_TIME", DoubleType(), True),  # Air time in minutes
    StructField("DISTANCE", DoubleType(), True)  # Distance in miles
])

# Column descriptions for documentation
COLUMN_DESCRIPTIONS = {
    "FL_DATE": "Flight date and time",
    "OP_UNIQUE_CARRIER": "Operating carrier code (e.g., AA, DL, UA)",
    "OP_CARRIER_FL_NUM": "Flight number",
    "ORIGIN_AIRPORT_ID": "Origin airport identifier",
    "ORIGIN": "Origin airport code (3-letter IATA code)",
    "DEST_AIRPORT_ID": "Destination airport identifier", 
    "DEST": "Destination airport code (3-letter IATA code)",
    "CRS_DEP_TIME": "Scheduled departure time (HHMM format)",
    "DEP_TIME": "Actual departure time (HHMM format)",
    "DEP_DELAY": "Departure delay in minutes (negative = early)",
    "DEP_DELAY_NEW": "New departure delay field",
    "CRS_ARR_TIME": "Scheduled arrival time (HHMM format)",
    "ARR_TIME": "Actual arrival time (HHMM format)",
    "ARR_DELAY": "Arrival delay in minutes (negative = early)",
    "ARR_DELAY_NEW": "New arrival delay field",
    "CANCELLED": "Cancellation flag (0=No, 1=Yes)",
    "DIVERTED": "Diversion flag (0=No, 1=Yes)",
    "AIR_TIME": "Air time in minutes",
    "DISTANCE": "Distance in miles"
}

# Key performance indicators and derived columns
PERFORMANCE_METRICS = [
    "DEP_DELAY",
    "ARR_DELAY", 
    "AIR_TIME",
    "DISTANCE",
    "CANCELLED",
    "DIVERTED"
]

# Categorical columns for grouping and analysis
CATEGORICAL_COLUMNS = [
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "FL_DATE"
]

# Time-based columns for temporal analysis
TIME_COLUMNS = [
    "FL_DATE",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "CRS_ARR_TIME", 
    "ARR_TIME"
]
