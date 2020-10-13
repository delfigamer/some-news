-- This file is only used for documentation, and is never executed directly
-- The actual code for setting up a database is located at src/Storage/Schema.hs
PRAGMA foreign_keys=ON;

BEGIN TRANSACTION;

CREATE TABLE sn_metadata
    ( mkey TEXT PRIMARY KEY
    , mvalue TEXT NOT NULL
    );

INSERT INTO sn_metadata VALUES
    ('schema_version', 'SomeNewsSchema 0 1');

CREATE TABLE sn_users
    ( user_id BLOB PRIMARY KEY
    , user_name TEXT NOT NULL
    , user_surname TEXT NOT NULL
    , user_join_date DATETIME NOT NULL
    , user_is_admin INTEGER NULL
    );

CREATE TABLE sn_access_keys
    ( access_key_front BLOB PRIMARY KEY
    , access_key_back_hash BLOB NOT NULL
    , access_key_user_id BLOB NOT NULL
        REFERENCES sn_users (user_id) ON UPDATE CASCADE ON DELETE CASCADE
    );
CREATE INDEX sn_access_keys_user_id_idx
    ON sn_access_keys (access_key_user_id);

CREATE TABLE sn_authors
    ( author_id BLOB PRIMARY KEY
    , author_name TEXT NOT NULL
    , author_description TEXT NOT NULL
    );

CREATE TABLE sn_author2user
    ( a2u_author_id BLOB NOT NULL
        REFERENCES sn_authors (author_id) ON UPDATE CASCADE ON DELETE CASCADE
    , a2u_user_id BLOB NOT NULL
        REFERENCES sn_users (user_id) ON UPDATE CASCADE ON DELETE CASCADE
    , PRIMARY KEY (a2u_author_id, a2u_user_id)
    );
CREATE INDEX sn_author2user_rev_idx
    ON sn_author2user (a2u_user_id, a2u_author_id);

CREATE TABLE sn_articles
    ( article_id BLOB PRIMARY KEY
    , article_author_id BLOB NULL
        REFERENCES sn_authors (author_id) ON UPDATE CASCADE ON DELETE SET NULL
    , article_name TEXT NOT NULL
    , article_text TEXT NOT NULL
    , article_publication_date DATETIME NULL
    );
CREATE INDEX sn_articles_publication_date_idx
    ON sn_articles (article_publication_date);

CREATE TABLE sn_files
    ( file_id BLOB PRIMARY KEY
    , file_name TEXT NULL
    , file_mimetype TEXT NOT NULL
    );

CREATE TABLE sn_file_chunks
    ( chunk_file_id BLOB NOT NULL
        REFERENCES sn_files (file_id) ON UPDATE CASCADE ON DELETE CASCADE
    , chunk_index INTEGER NOT NULL
    , chunk_content BLOB NOT NULL
    , PRIMARY KEY (chunk_file_id, chunk_index)
    );

COMMIT TRANSACTION;
