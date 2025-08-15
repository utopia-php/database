-- Create the root database if it doesn't exist
SELECT 'CREATE DATABASE root' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'root')\gexec

-- Connect to the root database
\c root;

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE root TO root; 