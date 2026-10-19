-- Existing databases contain deterministic fixture work. Preserve every job
-- and policy value, and prevent a model-directed worker from taking it over.
ALTER TABLE service_settings ADD COLUMN execution_profile text NOT NULL
    DEFAULT 'fixture-v1'
    CHECK (octet_length(execution_profile) BETWEEN 1 AND 256
        AND execution_profile !~ '[^ -~]');
