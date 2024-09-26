FROM python:3.12 AS hsds-base
# FROM hdfgroup/hdf5lib:1.14.0 as hsds-base

# Install Curl
RUN apt-get update; apt-get -y install curl

# Install HSDS
RUN mkdir /usr/local/src/hsds/ \
    /usr/local/src/hsds/hsds/ \
    /usr/local/src/hsds/admin/ \
    /usr/local/src/hsds/admin/config/ \
    /usr/local/src/hsds/hsds/util/ \
    /etc/hsds/ 

COPY pyproject.toml /usr/local/src/hsds/
COPY setup.cfg /user/local/src/hsds/
COPY hsds/*.py /usr/local/src/hsds/hsds/
COPY hsds/util/*.py /usr/local/src/hsds/hsds/util/
COPY admin/config/config.yml /etc/hsds/
COPY admin/config/config.yml /usr/local/src/hsds/admin/config/
COPY entrypoint.sh  /
RUN /bin/bash -c 'cd /usr/local/src/hsds; \
                  pip install build;\
                  python -m build;\
                  pip install -v . ;\
                  cd -'

EXPOSE 5100-5999
ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]
