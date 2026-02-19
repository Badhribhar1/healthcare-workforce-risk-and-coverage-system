import psycopg2


def get_connection():
    # Connecting to Postgresql Datatabse
    return psycopg2.connect(
        host="localhost",
        port=5433, 
        database="staffing_db",
        user="staffing_user",
        password="staffing_pass"
    )


def load_staff(df):

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO staff (
        staff_id,
        first_name,
        last_name,
        role,
        employment_type,
        home_unit,
        max_hours_per_week,
        hire_date
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print("Staff loaded successfully.")


def load_shifts(df):

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO shifts (
        shift_id,
        staff_id,
        unit,
        shift_date,
        shift_start,
        shift_end,
        shift_type,
        role,
        status
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print("Shifts loaded successfully.")


def load_census(df):

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO census (
        census_id,
        unit,
        date,
        total_patients,
        admissions,
        discharges
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print("Census loaded successfully.")


def load_timekeeping(df):

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO timekeeping (
        record_id,
        staff_id,
        week_start,
        hours_worked,
        overtime_hours,
        pto_hours,
        sick_hours
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print("Timekeeping loaded successfully.")
