CREATE TABLE IF NOT EXISTS ui_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL DEFAULT '#0a7ea4',
    group_name TEXT NOT NULL DEFAULT 'default',
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ui_record_annotations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_db TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_pk TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    priority TEXT NOT NULL DEFAULT 'P2',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_db, source_table, source_pk)
);

CREATE TABLE IF NOT EXISTS ui_record_tag_map (
    annotation_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(annotation_id, tag_id),
    FOREIGN KEY(annotation_id) REFERENCES ui_record_annotations(id) ON DELETE CASCADE,
    FOREIGN KEY(tag_id) REFERENCES ui_tags(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ui_saved_filters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    view_name TEXT NOT NULL,
    filter_name TEXT NOT NULL,
    filter_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(view_name, filter_name)
);

CREATE TABLE IF NOT EXISTS ui_deleted_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_db TEXT NOT NULL,
    source_table TEXT NOT NULL,
    key_col TEXT NOT NULL,
    key_val TEXT NOT NULL,
    row_json TEXT NOT NULL,
    deleted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    restored_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_ui_annotations_status ON ui_record_annotations(status);
CREATE INDEX IF NOT EXISTS idx_ui_annotations_priority ON ui_record_annotations(priority);
CREATE INDEX IF NOT EXISTS idx_ui_annotations_updated ON ui_record_annotations(updated_at);
CREATE INDEX IF NOT EXISTS idx_ui_deleted_records_deleted_at ON ui_deleted_records(deleted_at);
CREATE INDEX IF NOT EXISTS idx_ui_deleted_records_source ON ui_deleted_records(source_db, source_table);
