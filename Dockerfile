FROM python:3.6
#FROM python:3.6-slim

# Install vim, git, cron, and jdk
RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
    apt-get -y install cron && apt-get -y install git && apt-get install -y default-jdk

# Kafka:
RUN mkdir -p /kafka
ADD http://apache.claz.org/kafka/1.1.0/kafka_2.11-1.1.0.tgz /kafka
RUN tar -xzf /kafka/kafka_2.11-1.1.0.tgz
RUN pip install confluent-kafka==0.11.4

# place to keep our app and the data:
RUN mkdir -p /app
RUN mkdir -p /data

## Add crontab file in the cron directory
#ADD code/crontab /etc/cron.d/fetch-cron
## Give execution rights on the cron job
#RUN chmod 0644 /etc/cron.d/fetch-cron
## Apply cron job
#RUN crontab /etc/cron.d/fetch-cron
## Create the log file to be able to run tail
#RUN touch /var/log/cron.log

# install python libs
COPY kowalski/requirements.txt /app/
RUN pip install -r /app/requirements.txt

# copy over the secrets:
COPY secrets.json /app/

# copy over the code
ADD kowalski/ /app/

# change working directory to /app
WORKDIR /app

RUN git clone https://github.com/ZwickyTransientFacility/ztf-avro-alert.git

# run container
#CMD /usr/local/bin/supervisord -n -c supervisord.conf
#CMD cron && crontab /etc/cron.d/fetch-cron && /bin/bash
CMD /bin/bash
