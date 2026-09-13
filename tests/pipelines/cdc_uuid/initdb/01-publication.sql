-- Source table with a UUID column, plus the publication Sling's shared-reader
-- CDC expects. REPLICA IDENTITY FULL so UPDATE/DELETE carry old values.
CREATE TABLE public.cdc_uuid_test (
  id          INT PRIMARY KEY,
  external_id UUID,
  name        VARCHAR(100)
);

ALTER TABLE public.cdc_uuid_test REPLICA IDENTITY FULL;

CREATE PUBLICATION sling_cdc_uuid FOR TABLE public.cdc_uuid_test;

CREATE SCHEMA target_schema;
