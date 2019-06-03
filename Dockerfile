FROM python:3.6
#FROM python:3.6-slim

# Install vim, git, cron, and jdk
RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
    apt-get -y install cron && apt-get -y install git && apt-get install -y default-jdk

# Kafka:
RUN mkdir -p /kafka
#fixme: vvv
ADD http://apache.claz.org/kafka/2.2.0/kafka_2.11-2.2.0.tgz /kafka
RUN tar -xzf /kafka/kafka_2.11-2.2.0.tgz
#fixme: ^^^

# place to keep our app and the data:
RUN mkdir -p /app && mkdir -p /app/logs && mkdir -p /data && mkdir -p /_tmp && mkdir -p /matchfiles

## Add crontab file in the cron directory
#ADD code/crontab /etc/cron.d/fetch-cron
## Give execution rights on the cron job
#RUN chmod 0644 /etc/cron.d/fetch-cron
## Apply cron job
#RUN crontab /etc/cron.d/fetch-cron
## Create the log file to be able to run tail
#RUN touch /var/log/cron.log

# change working directory to /app
WORKDIR /app

# install python libs
COPY kowalski/requirements.txt /app/
RUN pip install -r /app/requirements.txt
# install bleeding-edge version of aiohttp first?
#RUN git clone https://github.com/aio-libs/aiohttp.git && /usr/local/bin/python /app/aiohttp/setup.py install && \
#    pip install -r /app/requirements.txt

# copy over the secrets:
COPY secrets.json /app/

# copy over the code
ADD kowalski/ /app/

#fixme: vvv
RUN git clone https://github.com/ZwickyTransientFacility/ztf-avro-alert.git
#fixme: ^^^

# generate keys
RUN python generate_secrets.py

# run tests
#RUN python -m pytest -s server.py

# run container
#CMD /usr/local/bin/supervisord -n -c supervisord.conf
#CMD cron && crontab /etc/cron.d/fetch-cron && /bin/bash
#CMD /bin/bash
#CMD /usr/local/bin/gunicorn -w 8 --bind 0.0.0.0:4000 --worker-tmp-dir /dev/shm --worker-class aiohttp.GunicornWebWorker server:app_factory
#CMD /usr/local/bin/gunicorn -w 12 --bind 0.0.0.0:4000 --worker-tmp-dir /dev/shm --worker-class aiohttp.GunicornUVLoopWebWorker server:app_factory
CMD /usr/local/bin/python server.py