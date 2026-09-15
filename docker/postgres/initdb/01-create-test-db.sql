-- Provisioned by the postgres image entrypoint on first init (empty pgdata).
-- Owned by POSTGRES_USER. Recreate with `docker compose down -v postgres`
-- followed by `docker compose up -d postgres` if the volume already exists.
CREATE DATABASE phlo_observer_test;
