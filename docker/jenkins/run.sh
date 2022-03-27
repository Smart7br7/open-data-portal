#!/bin/bash

docker run -d -p 8080:8080 -p 50000:50000 -v /home/jenkins/jenkins_home:/var/jenkins_home --restart=always br7etl
