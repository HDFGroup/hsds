FROM hdfgroup/python:3.8
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir /usr/local/src/hsds-src/ /usr/local/src/hsds/
COPY ./dist/hsds-*.tar.gz /usr/local/src/hsds-src
RUN pip install /usr/local/src/hsds-src/hsds-*.tar.gz --no-deps --no-binary hsds
RUN rm -rf /usr/local/src/hsds-src
RUN mkdir /etc/hsds/
COPY admin/config/config.yml /etc/hsds/
COPY entrypoint.sh  /

EXPOSE 5100-5999

ENTRYPOINT ["/entrypoint.sh"]
