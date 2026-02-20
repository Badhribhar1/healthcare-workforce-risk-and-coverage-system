import pandas as pd
from etl.data_upload import get_connection

def get_overtime_by_staff():
    # Calculates the overtime hours and percentage of overtime for each staff member
    query = """
    SELECT 
        staff_id,
        SUM(hours_worked) AS total_hours_worked,
        SUM(overtime_hours) AS total_overtime_hours,
        ROUND(
            (SUM(overtime_hours)::numeric / NULLIF(SUM(hours_worked), 0)) * 100,
            2
        ) AS overtime_percentage
    FROM timekeeping
    GROUP BY staff_id
    ORDER BY overtime_percentage DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df



def get_patient_to_staff_ratio():
    # Calculates the patient-to-staff ratio for each unit and shift date
    query = """
    SELECT
        c.unit,
        c.date AS shift_date,
        c.total_patients,
        COUNT(DISTINCT s.staff_id) AS staff_count,
        ROUND(
            c.total_patients::numeric /
            NULLIF(COUNT(DISTINCT s.staff_id), 0),
            2
        ) AS patient_to_staff_ratio
    FROM census c
    LEFT JOIN shifts s
        ON c.unit = s.unit
        AND c.date = s.shift_date
        AND s.status = 'Completed'
    GROUP BY
        c.unit,
        c.date,
        c.total_patients
    ORDER BY patient_to_staff_ratio DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_weekly_capacity_utilization():
    # Calculates the total hours worked, max hours allowed, and percentage of capacity utilized for each staff member by week
    query = """
    SELECT
        t.staff_id,
        s.first_name,
        s.last_name,
        t.week_start,
        SUM(t.hours_worked) AS total_hours_worked,
        s.max_hours_per_week,
        ROUND(
            (SUM(t.hours_worked)::numeric / NULLIF(s.max_hours_per_week, 0)) * 100,
            2
        ) AS percent_of_allowed_capacity
    FROM timekeeping t
    JOIN staff s
        ON t.staff_id = s.staff_id
    GROUP BY
        t.staff_id,
        s.first_name,
        s.last_name,
        t.week_start,
        s.max_hours_per_week
    ORDER BY percent_of_allowed_capacity DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_cancellation_rate_by_unit():
    # Calculates the number of cancelled shifts, total shifts, and cancellation rate percentage for each unit
    query = """
    SELECT
        unit,
        COUNT(*) FILTER (WHERE status = 'Cancelled') AS cancelled_shifts,
        COUNT(*) AS total_shifts,
        ROUND(
            COUNT(*) FILTER (WHERE status = 'Cancelled')::numeric
            / NULLIF(COUNT(*), 0) * 100,
            2
        ) AS cancellation_rate_percent
    FROM shifts
    GROUP BY unit
    ORDER BY cancellation_rate_percent DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df



def get_average_shift_duration():
    # Calculatres the average shift duration in hours for each staff member, considering only completed shifts
    query = """
    SELECT
        staff_id,
        ROUND(
            AVG(EXTRACT(EPOCH FROM (shift_end - shift_start)) / 3600),
            2
        ) AS avg_shift_duration_hours
    FROM shifts
    WHERE shift_end IS NOT NULL
    GROUP BY staff_id
    ORDER BY avg_shift_duration_hours DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df



def get_total_days_worked():
    # Calculates the total number of unique days worked for each staff member, considering only completed shifts
    query = """
    SELECT
        staff_id,
        COUNT(DISTINCT shift_date) AS total_days_worked
    FROM shifts
    WHERE status = 'Completed'
    GROUP BY staff_id
    ORDER BY total_days_worked DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df