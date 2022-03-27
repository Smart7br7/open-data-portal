# Beer Sheva Smart 7 CKAN Portal and ETL Pipeline

This repository contains the required code and configuration for setting up a CKAN backend and Jenkins server, similar to the one that are powering [Beer Sheva's Smart7 open data portal](https://www.beer-sheva.muni.il/OpenData/Pages/default.aspx). It does *not* contain anything related to the front-end app of the open data portal, which is an independent app.

In this repo you will find:
- Docker configuration for creating and running the CKAN data management platform
- Docker configuration for creating and running the Jenkins ETL platform
- processing code for:
  - Pulling data from ArcGIS into CKAN
  - Pulling data from an FTP server into CKAN
  - Pulling data from Sharepoint into CKAN
  - Converting data formats in CKAN
  - Pushing data from CKAN into data.gov.il

## Contents of this repository

### docker/ckan-portal

This folder contains a `docker-compose` file and matching Docker files and build environments for setting up a CKAN portal.

This CKAN deployment uses varnish as a caching proxy, and uses an Azure blob for file storage (you will need credentials for that). The required secrets should be updated in the `docker-compose/ckan-secrets.sh`.

This code is partly based on work done by @OriHoch [here](https://github.com/datopian/ckan-cloud-docker).

### docker/jenkins-etl

This folder contains the Dockerfile for running a fairly standard lts Jenkins server.

`./build.sh` will build the Docker image, and `./run.sh` will run it while mapping the local `/home/jenkins/jenkins_home` directory to the `/var/jenkins_home` directory in the container (to allow for persistent storage).

### etl

This folder contains various processing scripts which can be used inside the Jenkins server to perform various actions.

You can copy these to `/home/jenkins/jenkins_home` and then invoke them from a Jenkins job with the proper parameters.

Each script's functionality and code are individually documented in the script itself.
## License

See LICENSE for license information.

## Contributing

We welcome contributions to this repository. Please open an issue or pull request if you have any suggestions.