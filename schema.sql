CREATE TABLE IF NOT EXISTS staff (
    staff_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    role VARCHAR(20),
    employment_type VARCHAR(20),
    home_unit VARCHAR(50),
    max_hours_per_week INT,
    hire_date DATE
);

CREATE TABLE IF NOT EXISTS shifts (
    shift_id VARCHAR(20) PRIMARY KEY,
    staff_id VARCHAR(20),
    unit VARCHAR(50),
    shift_date DATE,
    shift_start TIMESTAMP,
    shift_end TIMESTAMP,
    shift_type VARCHAR(20),
    role VARCHAR(20),
    status VARCHAR(20),
    FOREIGN KEY (staff_id) REFERENCES staff(staff_id)
);

CREATE TABLE IF NOT EXISTS census (
    census_id VARCHAR(20) PRIMARY KEY,
    unit VARCHAR(50),
    date DATE,
    total_patients INT,
    admissions INT,
    discharges INT
);

CREATE TABLE IF NOT EXISTS timekeeping (
    record_id VARCHAR(20) PRIMARY KEY,
    staff_id VARCHAR(20),
    week_start DATE,
    hours_worked INT,
    overtime_hours INT,
    pto_hours INT,
    sick_hours INT,
    FOREIGN KEY (staff_id) REFERENCES staff(staff_id)
);
