CREATE TABLE IF NOT EXISTS listings (
    source      TEXT NOT NULL,
    listing_id  TEXT NOT NULL,
    url         TEXT,
    title       TEXT,
    brand       TEXT,
    cpu         TEXT,
    ram_gb      INT,
    storage_gb  INT,
    price       NUMERIC,
    province    TEXT,
    condition   TEXT,
    scraped_at  TIMESTAMP DEFAULT now(),
    PRIMARY KEY (source, listing_id)
);