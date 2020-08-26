FROM hdfgroup/python:3.8.5
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir /usr/local/src/hsds-src/ /usr/local/src/hsds/
COPY . /usr/local/src/hsds-src
RUN pip install /usr/local/src/hsds-src/ --no-deps
RUN rm -rf /usr/local/src/hsds-src
RUN mkdir /etc/hsds/
COPY admin/config/config.yml /etc/hsds/
COPY entrypoint.sh  /

EXPOSE 5100-5999

ENTRYPOINT ["/entrypoint.sh"]
