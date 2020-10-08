-- This file is only used for documentation, and is never executed directly
-- The actual code for setting up a database is located at src/Storage/Schema.hs
PRAGMA foreign_keys=ON;
BEGIN TRANSACTION;
CREATE TABLE sn_metadata
    ( mkey TEXT PRIMARY KEY
    , mvalue TEXT
    );
INSERT INTO sn_metadata VALUES
    ('schema_version', 'SomeNewsSchema 0 1');
CREATE TABLE sn_users
    ( user_id INTEGER PRIMARY KEY
    , user_name TEXT
    , user_surname TEXT
    );
COMMIT TRANSACTION;
