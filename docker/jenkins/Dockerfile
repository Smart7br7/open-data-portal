FROM jenkins/jenkins:lts-slim

USER root
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update
RUN apt-get install -y python3 python3-pip sudo nodejs yarn chromium 

RUN python3 -m pip install -U pip 
RUN python3 -m pip install requests fabric pyshp dataflows json2xml lxml fastkml "pyproj>=3" shapely google-auth azure-storage-blob dataflows-ckan
RUN apt-get install yarn
RUN yarn --version
RUN yarn add puppeteer

# drop back to the regular jenkins user - good practice
USER jenkins
 