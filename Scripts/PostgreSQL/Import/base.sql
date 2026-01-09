
-- Main import function
CREATE OR REPLACE PROCEDURE load_from_csv(table_to_import TEXT, file_path TEXT)
LANGUAGE plpgsql AS
$pr$
DECLARE
    schema_name text := 'public';
    set_clause text;
    where_clause text;
    col_list text[];
BEGIN
    -- Check if the file exists
    IF pg_stat_file(file_path, true) IS NULL THEN
        -- Log a warning that the file was not found and is being skipped.
        RAISE WARNING 'File not found: %. Skipping import for table %.', file_path, table_to_import;
        -- Exit the procedure gracefully, allowing the calling process to continue.
        RETURN;
    END IF;

    -- Keep the user informed
    RAISE INFO 'Importing content of table "%"...', table_to_import;

    -- Load CSV to table
    EXECUTE format('COPY %I FROM %L WITH (FORMAT CSV, HEADER MATCH, NULL '''', ENCODING ''UTF8'');', table_to_import, file_path);

    -- Retrieve nullable columns with their DEFAULT values
    SELECT INTO set_clause, where_clause
        string_agg(
            format(
                '%I = CASE WHEN %I IS NULL THEN %s ELSE %I END',
                metadata.column_name,
                metadata.column_name,
                -- Use the column's DEFAULT value directly
                -- Remove casting if present to avoid syntax errors
                split_part(metadata.column_default, '::', 1),
                metadata.column_name
            ),
            ', '
        ),
        string_agg(format('%I IS NULL', metadata.column_name), ' OR ')
    FROM (
        SELECT column_name, column_default
        FROM information_schema.columns c
        WHERE table_schema = schema_name
            AND table_name = table_to_import
            AND is_nullable = 'YES'
            AND column_default IS NOT NULL
    ) AS metadata;

    -- Apply DEFAULT values to columns with NULLs
    -- NOTE: This can take up to 20% of the import time.
    IF set_clause IS NOT NULL THEN
        EXECUTE format('UPDATE %I.%I SET %s WHERE %s;', schema_name, table_to_import, set_clause, where_clause);
    END IF;
END
$pr$;


-- Auxiliary table that works as a base for enumerations
DROP TABLE IF EXISTS enum_table;
CREATE TABLE enum_table (
    id INTEGER PRIMARY KEY CHECK (id >= 0),
    name TEXT
);

-- Create a trigger that prevents row insertion
CREATE OR REPLACE FUNCTION prevent_insertion() RETURNS TRIGGER
LANGUAGE plpgsql AS
$func$
BEGIN
    RAISE EXCEPTION 'Cannot insert into the table, since it is intended as a base for others.';
END
$func$;

-- Attach the trigger to the table
CREATE OR REPLACE TRIGGER enum_insertion
BEFORE INSERT ON enum_table
EXECUTE FUNCTION prevent_insertion();


-- Helper function to parse GTFS time strings
CREATE OR REPLACE FUNCTION parse_gtfs_time(time_str text) RETURNS INTERVAL
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    hrs int;
    mins int;
    secs int;
BEGIN
    IF time_str IS NULL THEN
        RETURN NULL;
    ELSE
        hrs  := split_part(time_str, ':', 1)::int;
        mins := split_part(time_str, ':', 2)::int;
        secs := split_part(time_str, ':', 3)::int;
        RETURN make_interval(hours => hrs, mins => mins, secs => secs);
    END IF;
END;
$$;
