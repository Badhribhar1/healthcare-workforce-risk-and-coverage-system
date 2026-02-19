import pandas as pd
import os
from data_upload import *


def cleanse_census_data(df):
    # Null Check
    if len(df) == 0:
        raise ValueError("Input dataframe is empty.")
    
    # Column Check
    required_columns = ['census_id', 'unit', 'date', 'total_patients', 'admissions', 'discharges']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Duplicates Check
    if df["census_id"].duplicated().any():
        raise ValueError("Duplicate census_id detected")
    
    # Data Type Converstions
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["total_patients"] = pd.to_numeric(df["total_patients"], errors="coerce")
    df["admissions"] = pd.to_numeric(df["admissions"], errors="coerce")
    df["discharges"] = pd.to_numeric(df["discharges"], errors="coerce")
    df["unit"] = df["unit"].str.strip().str.upper()

    # NAN checks
    invalid_rows = df[df[["date", "total_patients", "admissions", "discharges"]].isna().any(axis=1)]
    invalid_ratio = len(invalid_rows) / len(df)
    if invalid_ratio > 0.2:
        raise ValueError("More than 20% rows invalid. Failing job.")
    if len(invalid_rows) > 0:
        print(f"Dropping {len(invalid_rows)} rows due to NaN values.")
    df = df.drop(invalid_rows.index)
    df = df.reset_index(drop=True)
    os.makedirs("logs", exist_ok=True)
    invalid_rows.assign(error_reason="Missing required fields").to_csv(
    "logs/census_rejected_rows.csv",
    mode="a",
    header=not os.path.exists("logs/census_rejected_rows.csv"),
    index=False
    )   

    # Negative Values Check
    invalid_neg = df[(df["total_patients"] < 0) | (df["admissions"] < 0) | (df["discharges"] < 0)]
    if not invalid_neg.empty:
        invalid_neg.assign(error_reason="Negative values").to_csv(
        "logs/census_rejected_rows_negative.csv",
        mode="a",
        header=not os.path.exists("logs/census_rejected_rows_negative.csv"),
        index=False
        )
        df = df.drop(invalid_neg.index)
        df = df.reset_index(drop=True)


    # Admission-Discharge Consistency Check
    invalid_mask = (
        (df["discharges"] > df["total_patients"]) |
        (df["admissions"] > df["total_patients"])
    )
    invalid_discharges_and_admissions = df[invalid_mask]
    if not invalid_discharges_and_admissions.empty:
        raise ValueError(f"Found invalid discharge or admission values: {invalid_discharges_and_admissions}")

    # Date Range Check:
    # today = pd.to_datetime.today().normalize()
    today = pd.to_datetime("2026-03-01")
    if (df["date"] > today).any():
        raise ValueError("Future dates detected in census data.")
    dup_check = df.duplicated(subset=["unit", "date"]).any()
    if dup_check:
        raise ValueError("Found duplicate unit-date combinations")
    
    return df



def cleanse_shift_schedule(df):
    # Null Check
    if len(df) == 0:
        raise ValueError("Input dataframe is empty.")
    
    # Column Check
    required_columns = [
        'shift_id', 'staff_id', 'unit',
        'shift_date', 'shift_start', 'shift_end',
        'shift_type', 'role', 'status'
    ]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Duplicate Primary Key Check
    if df["shift_id"].duplicated().any():
        raise ValueError("Duplicate shift_id detected")
    
    # Data Type Conversions
    df["shift_date"] = pd.to_datetime(df["shift_date"], errors="coerce")
    df["shift_start"] = pd.to_datetime(df["shift_start"], errors="coerce")
    df["shift_end"] = pd.to_datetime(df["shift_end"], errors="coerce")

    df["unit"] = df["unit"].str.strip().str.upper()
    df["shift_type"] = df["shift_type"].str.strip().str.lower()
    df["role"] = df["role"].str.strip().str.upper()
    df["status"] = df["status"].str.strip().str.lower()


    # NaN checks (critical timestamp + FK fields)
    invalid_rows = df[df[
        ["shift_date", "shift_start", "shift_end", "shift_id", "staff_id"]
    ].isna().any(axis=1)]
    invalid_ratio = len(invalid_rows) / len(df)
    if invalid_ratio > 0.2:
        raise ValueError("More than 20% rows invalid. Failing job.")
    if len(invalid_rows) > 0:
        print(f"Dropping {len(invalid_rows)} rows due to NaN values.")
    df = df.drop(invalid_rows.index)
    df = df.reset_index(drop=True)
    os.makedirs("logs", exist_ok=True)
    invalid_rows.assign(error_reason="Missing required fields").to_csv(
        "logs/shift_rejected_rows.csv",
        mode="a",
        header=not os.path.exists("logs/shift_rejected_rows.csv"),
        index=False
    )

    # Logical Check
    invalid_time = df[df["shift_end"] <= df["shift_start"]]
    if not invalid_time.empty:
        invalid_time.assign(error_reason="Invalid shift time range").to_csv(
            "logs/shift_rejected_rows_time.csv",
            mode="a",
            header=not os.path.exists("logs/shift_rejected_rows_time.csv"),
            index=False
        )
        df = df.drop(invalid_time.index)
        df = df.reset_index(drop=True)

    # Date Range Check (no future shift dates)
    # today = pd.to_datetime.today().normalize()
    today = pd.to_datetime("2026-03-01")
    if (df["shift_date"] > today).any():
        raise ValueError("Future shift dates detected.")

    return df



def cleanse_staff_master(df):
    # Null Check
    if len(df) == 0:
        raise ValueError("Input dataframe is empty.")
    
    # Column Check
    required_columns = [
        'staff_id', 'first_name', 'last_name',
        'role', 'employment_type',
        'home_unit', 'max_hours_per_week', 'hire_date'
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Duplicates Check
    if df["staff_id"].duplicated().any():
        raise ValueError("Duplicate staff_id detected")

    # Type Conversions
    df["max_hours_per_week"] = pd.to_numeric(df["max_hours_per_week"], errors="coerce")
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")

    df["first_name"] = df["first_name"].str.strip().str.capitalize()
    df["last_name"] = df["last_name"].str.strip().str.capitalize()
    df["role"] = df["role"].str.strip().str.upper()
    df["employment_type"] = df["employment_type"].str.strip().str.upper()
    df["home_unit"] = df["home_unit"].str.strip().str.upper()

    # NaN Checks 
    invalid_rows = df[df[
        ["staff_id", "first_name", "last_name",
         "role", "employment_type",
         "max_hours_per_week", "hire_date"]
    ].isna().any(axis=1)]

    invalid_ratio = len(invalid_rows) / len(df)

    if invalid_ratio > 0.2:
        raise ValueError("More than 20% rows invalid. Failing job.")

    if len(invalid_rows) > 0:
        print(f"Dropping {len(invalid_rows)} rows due to NaN values.")

    df = df.drop(invalid_rows.index)
    df = df.reset_index(drop=True)

    os.makedirs("logs", exist_ok=True)
    invalid_rows.assign(error_reason="Missing required fields").to_csv(
        "logs/staff_rejected_rows.csv",
        mode="a",
        header=not os.path.exists("logs/staff_rejected_rows.csv"),
        index=False
    )

    # Negative Values Check
    invalid_hours = df[df["max_hours_per_week"] < 0]

    if not invalid_hours.empty:
        invalid_hours.assign(error_reason="Negative max_hours_per_week").to_csv(
            "logs/staff_rejected_rows_hours.csv",
            mode="a",
            header=not os.path.exists("logs/staff_rejected_rows_hours.csv"),
            index=False
        )
        df = df.drop(invalid_hours.index)
        df = df.reset_index(drop=True)

    # Date Range Check
    # today = pd.to_datetime.today().normalize()
    today = pd.to_datetime("2026-03-01")
    if (df["hire_date"] > today).any():
        raise ValueError("Future hire_date detected.")

    return df



def cleanse_time_keeping(df):
    # Null Check
    if len(df) == 0:
        raise ValueError("Input dataframe is empty.")
    
    # Column Check
    required_columns = [
        'record_id', 'staff_id', 'week_start',
        'hours_worked', 'overtime_hours',
        'pto_hours', 'sick_hours'
    ]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Duplicates Check
    if df["record_id"].duplicated().any():
        raise ValueError("Duplicate record_id detected")

    # Type Conversions
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    df["hours_worked"] = pd.to_numeric(df["hours_worked"], errors="coerce")
    df["overtime_hours"] = pd.to_numeric(df["overtime_hours"], errors="coerce")
    df["pto_hours"] = pd.to_numeric(df["pto_hours"], errors="coerce")
    df["sick_hours"] = pd.to_numeric(df["sick_hours"], errors="coerce")

    # NaN checks
    invalid_rows = df[df[
        ["record_id", "staff_id", "week_start",
         "hours_worked", "overtime_hours",
         "pto_hours", "sick_hours"]
    ].isna().any(axis=1)]
    invalid_ratio = len(invalid_rows) / len(df)
    if invalid_ratio > 0.2:
        raise ValueError("More than 20% rows invalid. Failing job.")
    if len(invalid_rows) > 0:
        print(f"Dropping {len(invalid_rows)} rows due to NaN values.")
    df = df.drop(invalid_rows.index)
    df = df.reset_index(drop=True)
    os.makedirs("logs", exist_ok=True)
    invalid_rows.assign(error_reason="Missing required fields").to_csv(
        "logs/timekeeping_rejected_rows.csv",
        mode="a",
        header=not os.path.exists("logs/timekeeping_rejected_rows.csv"),
        index=False
    )

    # Negative values check
    invalid_neg = df[
        (df["hours_worked"] < 0) |
        (df["overtime_hours"] < 0) |
        (df["pto_hours"] < 0) |
        (df["sick_hours"] < 0)
    ]
    if not invalid_neg.empty:
        invalid_neg.assign(error_reason="Negative hour values").to_csv(
            "logs/timekeeping_rejected_rows_negative.csv",
            mode="a",
            header=not os.path.exists("logs/timekeeping_rejected_rows_negative.csv"),
            index=False)
        df = df.drop(invalid_neg.index)
        df = df.reset_index(drop=True)

    # Overtime Logic Check
    invalid_overtime = df[df["overtime_hours"] > df["hours_worked"]]
    if not invalid_overtime.empty:
        raise ValueError("Overtime hours exceed total hours worked.")

    # Date Range Check
    # today = pd.to_datetime.today().normalize()
    today = pd.to_datetime("2026-03-01")
    if (df["week_start"] > today).any():
        raise ValueError("Future week_start detected.")

    return df


def choose_file(filename):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    
    for root, dir, files in os.walk(data_dir):
        if filename in files:
            filepath = os.path.join(root, filename)
            
            if filepath.endswith('.csv'):
                return pd.read_csv(filepath)
            
            elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
                return pd.read_excel(filepath)
            
            else:
                raise ValueError(f"Unsupported file format for {filename}")
    
    raise FileNotFoundError(f"File '{filename}' not found in {data_dir} or its subdirectories")




def validate_dataframe(df, pk_col=None, required_cols=None):
    print("----- DATAFRAME VALIDATION -----")
    print(f"Total rows: {len(df)}")
    print()

    # Null summary
    print("Null Counts:")
    print(df.isnull().sum())
    print()

    # Duplicate PK
    if pk_col:
        dup_count = df[pk_col].duplicated().sum()
        print(f"Duplicate {pk_col}: {dup_count}")
        print()

    # Duplicate full rows
    full_dups = df.duplicated().sum()
    print(f"Fully duplicated rows: {full_dups}")
    print()

    # Required column check
    if required_cols:
        missing = [col for col in required_cols if col not in df.columns]
        print(f"Missing required columns: {missing}")
        print()

    print("Dtypes:")
    print(df.dtypes)
    print("--------------------------------")



if __name__ == "__main__":
    df1 = choose_file('census_daily_week_01.csv')
    df1 = cleanse_census_data(df1)
    # print(df1)
    # print(df1.dtypes) 
    
    df2 = choose_file('shift_schedule_week_01.csv')
    df2 = cleanse_shift_schedule(df2)
    # print(df2)
    # print(df2.dtypes)

    df3 = choose_file('staff_master.csv')
    df3 = cleanse_staff_master(df3)
    # print(df3)
    # print(df3.dtypes)

    df4 = choose_file('timekeeping_week_01.csv')
    df4 = cleanse_time_keeping(df4)
    # print(df4)
    # print(df4.dtypes)

    # validate_dataframe(df1, pk_col="census_id", required_cols=['census_id', 'unit', 'date', 'total_patients', 'admissions', 'discharges'])
    # validate_dataframe(df2, pk_col="shift_id", required_cols=['shift_id', 'staff_id', 'unit', 'shift_date', 'shift_start', 'shift_end', 'shift_type', 'role', 'status'])
    # validate_dataframe(df3, pk_col="staff_id", required_cols=['staff_id', 'first_name', 'last_name', 'role', 'employment_type', 'home_unit', 'max_hours_per_week', 'hire_date'])
    # validate_dataframe(df4, pk_col="record_id", required_cols=['record_id', 'staff_id', 'week_start', 'hours_worked', 'overtime_hours', 'pto_hours', 'sick_hours'])
    print("About to load staff...")
    load_staff(df3)
    print("Load complete.") 
    print("About to load shifts...")
    load_shifts(df2)
    print("Load complete.") 
    print("About to load census...")
    load_census(df1)
    print("Load complete.") 
    print("About to load timekeeping...")
    load_timekeeping(df4)
    print("Load complete.") 
