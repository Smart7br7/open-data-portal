#!/bin/sh
# Copy another CKAN's contents to this one
# Set ADMIN_API_KEY to API key for the target CKAN
# Set MIGRATE_FROM to the CKAN to copy from

docker-compose exec ckan /bin/sh -c ". venv/bin/activate; pip install ckanapi; ADMIN_API_KEY=$ADMIN_API_KEY MIGRATE_FROM=$MIGRATE_FROM python /migrate.py"
