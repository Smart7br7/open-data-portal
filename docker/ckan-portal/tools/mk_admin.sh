#!/bin/bash
# Add an admin user to this CKAN, set PASSWORD for the password

docker-compose exec ckan ckan-paster --plugin=ckan \
    sysadmin add -c /etc/ckan/production.ini admin password='$PASSWORD' email=admin@localhost