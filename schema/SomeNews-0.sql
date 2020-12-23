-- This file is only used for documentation, and is never executed directly
-- The actual code for setting up a database is located at src/Storage/Schema.hs
PRAGMA foreign_keys=ON;
-- 111
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
    , user_is_admin BOOLEAN NOT NULL
    );

CREATE TABLE sn_access_keys
    ( access_key_id BLOB PRIMARY KEY
    , access_key_hash BLOB NOT NULL
    , access_key_user_id BLOB NOT NULL
        REFERENCES sn_users (user_id) ON UPDATE CASCADE ON DELETE CASCADE
    );
CREATE INDEX sn_access_keys_user_id_idx
    ON sn_access_keys (access_key_user_id, access_key_id);

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

CREATE TABLE sn_categories
    ( category_id BLOB PRIMARY KEY
    , category_name TEXT NOT NULL
    , category_parent_id BLOB
        REFERENCES sn_categories (category_id) ON UPDATE CASCADE ON DELETE NO ACTION
    );
CREATE INDEX sn_categories_parent_idx
    ON sn_categories (category_parent_id, category_id);

CREATE TABLE sn_articles
    ( article_id BLOB PRIMARY KEY
    , article_version BLOB NOT NULL
    , article_author_id BLOB
        REFERENCES sn_authors (author_id) ON UPDATE CASCADE ON DELETE SET NULL
    , article_name TEXT NOT NULL
    , article_text TEXT NOT NULL
    , article_publication_date DATETIME NOT NULL
    , article_category_id BLOB
        REFERENCES sn_categories (category_id) ON UPDATE CASCADE ON DELETE NO ACTION
    );
CREATE INDEX sn_articles_main_idx
    ON sn_articles (article_publication_date);
CREATE INDEX sn_articles_author_idx
    ON sn_articles (article_author_id, article_publication_date);
CREATE INDEX sn_articles_category_idx
    ON sn_articles (article_category_id, article_publication_date);

CREATE TABLE sn_tags
    ( tag_id BLOB PRIMARY KEY
    , tag_name TEXT NOT NULL
    );

CREATE TABLE sn_article2tag
    ( a2t_article_id BLOB NOT NULL
        REFERENCES sn_articles (article_id) ON UPDATE CASCADE ON DELETE CASCADE
    , a2t_tag_id BLOB NOT NULL
        REFERENCES sn_tags (tag_id) ON UPDATE CASCADE ON DELETE CASCADE
    , PRIMARY KEY (a2t_article_id, a2t_tag_id)
    );
CREATE INDEX sn_article2tag_rev_idx
    ON sn_article2tag (a2t_tag_id, a2t_article_id);

CREATE TABLE sn_comments
    ( comment_id BLOB PRIMARY KEY
    , comment_article_id BLOB NOT NULL
        REFERENCES sn_articles (article_id) ON UPDATE CASCADE ON DELETE CASCADE
    , comment_user_id BLOB
        REFERENCES sn_users (user_id) ON UPDATE CASCADE ON DELETE SET NULL
    , comment_text TEXT NOT NULL
    , comment_date DATETIME NOT NULL
    , comment_edit_date DATETIME
    );
CREATE INDEX sn_comments_article_idx
    ON sn_comments (comment_article_id, comment_date);
CREATE INDEX sn_comments_user_idx
    ON sn_comments (comment_user_id, comment_date);

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
