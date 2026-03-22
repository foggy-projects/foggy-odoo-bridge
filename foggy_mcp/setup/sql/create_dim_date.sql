-- ═══════════════════════════════════════════════════════════════════
-- Foggy Date Dimension Table (PostgreSQL)
--
-- Pre-populated calendar table for time-based aggregations
-- (year, quarter, month, week, day of week, etc.)
--
-- Usage:
--   SELECT create_or_refresh_dim_date(2020, 2035);
--
-- Default range: 2020-01-01 to 2035-12-31 (16 years, ~5844 rows)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_date (
    date_key       INTEGER     NOT NULL PRIMARY KEY,   -- YYYYMMDD format
    full_date      DATE        NOT NULL UNIQUE,
    year           INTEGER     NOT NULL,
    quarter        INTEGER     NOT NULL,               -- 1-4
    month          INTEGER     NOT NULL,               -- 1-12
    month_name     VARCHAR(20) NOT NULL,               -- January, February, ...
    month_short    VARCHAR(3)  NOT NULL,               -- Jan, Feb, ...
    week_of_year   INTEGER     NOT NULL,               -- ISO week 1-53
    day_of_month   INTEGER     NOT NULL,               -- 1-31
    day_of_week    INTEGER     NOT NULL,               -- 0=Mon, 6=Sun (ISO)
    day_name       VARCHAR(20) NOT NULL,               -- Monday, Tuesday, ...
    day_short      VARCHAR(3)  NOT NULL,               -- Mon, Tue, ...
    is_weekend     BOOLEAN     NOT NULL DEFAULT FALSE,
    year_month     VARCHAR(7)  NOT NULL,               -- 2024-01
    year_quarter   VARCHAR(7)  NOT NULL                -- 2024-Q1
);

CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date (year);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date (year, month);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_quarter ON dim_date (year, quarter);

CREATE OR REPLACE FUNCTION create_or_refresh_dim_date(
    start_year INTEGER DEFAULT 2020,
    end_year   INTEGER DEFAULT 2035
) RETURNS INTEGER AS $$
DECLARE
    d         DATE;
    end_date  DATE;
    row_count INTEGER := 0;
BEGIN
    d := make_date(start_year, 1, 1);
    end_date := make_date(end_year, 12, 31);

    -- Delete existing rows in range (allow incremental refresh)
    DELETE FROM dim_date
    WHERE full_date >= d AND full_date <= end_date;

    WHILE d <= end_date LOOP
        INSERT INTO dim_date (
            date_key, full_date, year, quarter, month, month_name, month_short,
            week_of_year, day_of_month, day_of_week, day_name, day_short,
            is_weekend, year_month, year_quarter
        ) VALUES (
            to_char(d, 'YYYYMMDD')::INTEGER,
            d,
            EXTRACT(YEAR FROM d)::INTEGER,
            EXTRACT(QUARTER FROM d)::INTEGER,
            EXTRACT(MONTH FROM d)::INTEGER,
            to_char(d, 'Month'),
            to_char(d, 'Mon'),
            EXTRACT(WEEK FROM d)::INTEGER,
            EXTRACT(DAY FROM d)::INTEGER,
            EXTRACT(ISODOW FROM d)::INTEGER - 1,  -- 0=Mon, 6=Sun
            to_char(d, 'Day'),
            to_char(d, 'Dy'),
            EXTRACT(ISODOW FROM d)::INTEGER >= 6,
            to_char(d, 'YYYY-MM'),
            to_char(d, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM d)::TEXT
        ) ON CONFLICT (date_key) DO NOTHING;

        d := d + INTERVAL '1 day';
        row_count := row_count + 1;
    END LOOP;

    RETURN row_count;
END;
$$ LANGUAGE plpgsql;
