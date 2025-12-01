CREATE TABLE IF NOT EXISTS counters (
    id SERIAL PRIMARY KEY,
    counter_id TEXT UNIQUE NOT NULL,         -- identifiant du compteur (open data)
    name TEXT,                               -- nom du compteur
    location TEXT                            -- optionnel : description ou coordonnée
);

CREATE TABLE IF NOT EXISTS counts (
    id SERIAL PRIMARY KEY,
    counter_id TEXT NOT NULL,                -- FK logique vers counters.counter_id
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- timestamp de la mesure
    count INTEGER NOT NULL,                  -- valeur du comptage
    CONSTRAINT uniq_counter_ts UNIQUE (counter_id, ts)
);
