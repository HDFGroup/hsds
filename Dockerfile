FROM python:3.9-slim AS hsds-base

# Install HSDS
RUN mkdir /usr/local/src/hsds/ \
    /usr/local/src/hsds/hsds/ \
    /usr/local/src/hsds/admin/ \
    /usr/local/src/hsds/admin/config/ \
    /usr/local/src/hsds/hsds/util/ \
    /etc/hsds/ 

COPY setup.py /usr/local/src/hsds/
COPY hsds/*.py /usr/local/src/hsds/hsds/
COPY hsds/util/*.py /usr/local/src/hsds/hsds/util/
COPY admin/config/config.yml /etc/hsds/
COPY admin/config/config.yml /usr/local/src/hsds/admin/config/
COPY entrypoint.sh  /
RUN /bin/bash -c 'cd /usr/local/src/hsds; pip install -e ".[azure]" ; cd -'

EXPOSE 5100-5999
ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]
