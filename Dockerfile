FROM hdfgroup/python:3.5
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir /usr/local/src/hsds
COPY hsds /usr/local/src/hsds
COPY entrypoint.sh  /

EXPOSE 5000-6000
 
ENTRYPOINT ["/entrypoint.sh"]