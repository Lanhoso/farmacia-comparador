-- ============================================================
-- supabase_schema.sql — farmaciabarata.cl
-- Execute no SQL Editor do Supabase (dashboard.supabase.com)
-- ============================================================


-- ── Tabela: farmacias ────────────────────────────────────────
-- Lookup table com os IDs canônicos, nomes e logos das farmácias.

CREATE TABLE IF NOT EXISTS farmacias (
    id       VARCHAR(20)  PRIMARY KEY,   -- "cruz_verde" | "salcobrand" | "ahumada"
    nombre   VARCHAR(100) NOT NULL,
    url_logo VARCHAR(500)
);

-- Seed inicial das 3 farmácias
INSERT INTO farmacias (id, nombre, url_logo) VALUES
    ('cruz_verde',  'Cruz Verde',  NULL),
    ('salcobrand',  'Salcobrand',  NULL),
    ('ahumada',     'Ahumada',     NULL)
ON CONFLICT (id) DO NOTHING;


-- ── Tabela: precios_hoy ──────────────────────────────────────
-- Armazena o estado mais atual de cada SKU por farmácia.
-- UPSERT usa (sku, farmacia_id) como chave de conflito.

CREATE TABLE IF NOT EXISTS precios_hoy (

    -- Identificação do produto
    sku               VARCHAR(100),
    ean_code          VARCHAR(13),

    -- Dados do produto
    nombre_producto   VARCHAR(500),
    principio_activo  VARCHAR(300),
    laboratorio       VARCHAR(200),
    presentacion      VARCHAR(200),
    cantidad          INTEGER,
    dosis             VARCHAR(50),

    -- Atributos regulatórios
    is_bioequivalente BOOLEAN      NOT NULL DEFAULT FALSE,
    requiere_receta   BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Farmácia e preços
    farmacia_id       VARCHAR(20)  NOT NULL REFERENCES farmacias(id),
    precio_original   INTEGER,
    precio_actual     INTEGER,

    -- URLs
    url_product       VARCHAR(1000),
    url_image         VARCHAR(1000),

    -- Metadados
    scraped_at        TIMESTAMPTZ  NOT NULL,

    -- Chave primária composta
    PRIMARY KEY (sku, farmacia_id),

    -- Garantia: precio_atual nunca maior que precio_original
    CONSTRAINT check_precio
        CHECK (precio_actual IS NULL OR precio_original IS NULL OR precio_actual <= precio_original)
);

-- Índices para queries comuns no site
CREATE INDEX IF NOT EXISTS idx_precios_farmacia    ON precios_hoy (farmacia_id);
CREATE INDEX IF NOT EXISTS idx_precios_principio   ON precios_hoy (principio_activo);
CREATE INDEX IF NOT EXISTS idx_precios_precio      ON precios_hoy (precio_actual);
CREATE INDEX IF NOT EXISTS idx_precios_scraped_at  ON precios_hoy (scraped_at DESC);
