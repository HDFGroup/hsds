FROM hdfgroup/python:3.8
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir /usr/local/src/hsds-src/ /usr/local/src/hsds/
COPY . /usr/local/src/hsds-src
RUN pip install /usr/local/src/hsds-src/[azure]
COPY admin/config/passwd.txt /usr/local/src/hsds/
COPY entrypoint.sh  /

EXPOSE 5100-5999
 
ENTRYPOINT ["/entrypoint.sh"]
