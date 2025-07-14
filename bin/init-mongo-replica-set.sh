#!/bin/sh

set -e

echo "Waiting for MongoDB to be ready..."
until docker compose exec mongo mongosh --eval "print('MongoDB is ready')" > /dev/null 2>&1; do
    sleep 1
done

echo "Initializing MongoDB replica set..."

# First, initialize the replica set without authentication
echo "Initializing replica set..."
docker compose exec mongo mongosh --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongo:27017"}]})'

# Wait for the replica set to be ready
echo "Waiting for replica set to be ready..."
until docker compose exec mongo mongosh --eval "rs.status().ok" | grep -q "1"; do
    sleep 2
done

echo "Replica set initialized successfully!"

# Now create the admin user and enable authentication
echo "Creating admin user and enabling authentication..."
docker compose exec mongo mongosh --eval 'use admin; db.createUser({user: "root", pwd: "password", roles: [{role: "root", db: "admin"}]})'

# Test authentication
echo "Testing authentication..."
docker compose exec mongo mongosh admin -u root -p password --eval 'db.runCommand({ping: 1})'

echo "MongoDB replica set is ready for transactions!" 