FROM hdfgroup/python:3.6
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir  /usr/local/src/ 
COPY write_slice.py /usr/local/src
COPY config.py /usr/local/src
COPY entrypoint.sh  /

ENTRYPOINT ["/entrypoint.sh"]
