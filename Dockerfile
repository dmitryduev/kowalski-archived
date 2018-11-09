#FROM python:3.6
FROM python:3.6-slim

# Install vim, git, and cron
RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
    apt-get -y install cron && apt-get -y install git

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
COPY code/requirements.txt /app/
RUN pip install -r /app/requirements.txt

# copy over the secrets:
COPY secrets.json /app/

# copy over the code
ADD code/ /app/

# change working directory to /app
WORKDIR /app

# run server with gunicorn
#CMD /usr/local/bin/supervisord -n -c supervisord.conf
#CMD cron && crontab /etc/cron.d/fetch-cron && /bin/bash
CMD /bin/bash
