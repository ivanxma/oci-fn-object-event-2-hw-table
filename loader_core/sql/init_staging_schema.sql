-- Dedicated transient loader schema. __STAGING_DATABASE__ is replaced only
-- with a validated, quoted identifier by deploy/initialize_databases.py.
CREATE DATABASE IF NOT EXISTS __STAGING_DATABASE__ CHARACTER SET utf8mb4;
