-- The tables belong to postgres. cdc_reader has only REPLICATION and SELECT,
-- so information_schema hides their constraints from it.
CREATE SCHEMA src;
CREATE SCHEMA tgt;

-- Key with two columns, in an order that is not the column order.
CREATE TABLE src.orders (
  tenant_id int NOT NULL,
  order_id  int NOT NULL,
  name      text,
  PRIMARY KEY (order_id, tenant_id)
);

-- No primary key. The replica identity is a unique index.
CREATE TABLE src.events_ri (
  event_id int NOT NULL,
  payload  text
);
CREATE UNIQUE INDEX events_ri_uk ON src.events_ri (event_id);
ALTER TABLE src.events_ri REPLICA IDENTITY USING INDEX events_ri_uk;

-- Primary key, but the replica identity is a different unique index.
-- A DELETE logs only code, so code is the key.
CREATE TABLE src.items_alt (
  id   int PRIMARY KEY,
  code text NOT NULL,
  name text
);
CREATE UNIQUE INDEX items_alt_code_uk ON src.items_alt (code);
ALTER TABLE src.items_alt REPLICA IDENTITY USING INDEX items_alt_code_uk;

-- No key at all. FULL lets the reader start, but the merge has no key.
CREATE TABLE src.no_key (a int, b text);
ALTER TABLE src.no_key REPLICA IDENTITY FULL;

CREATE PUBLICATION sling_cdc_pk FOR TABLE src.orders, src.events_ri, src.items_alt, src.no_key;

CREATE ROLE cdc_reader LOGIN REPLICATION PASSWORD 'cdc_reader_pw';
GRANT USAGE ON SCHEMA src TO cdc_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA src TO cdc_reader;
