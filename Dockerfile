FROM continuumio/miniconda3@sha256:7838d0ce65783b0d944c19d193e2e6232196bada9e5f3762dc7a9f07dc271179 AS hsds-base
LABEL maintainer="Aleksandar Jelenak <help@hdfgroup.org>"
RUN conda update conda -y && \
    conda config --add channels conda-forge && \
    conda config --set channel_priority strict && \
    conda install conda-pack && \
    conda create --name hsds --yes python=3.8
RUN conda install --name hsds --yes \
        pip \
        wheel \
        curl
         
RUN conda-pack -n hsds -o /tmp/hsds-env.tar \
    && mkdir -p /opt/env/hsds \
    && cd /opt/env/hsds \
    && tar xf /tmp/hsds-env.tar \
    && rm /tmp/hsds-env.tar
RUN /opt/env/hsds/bin/conda-unpack


FROM debian:buster-slim AS hsds
LABEL maintainer="Aleksandar Jelenak <help@hdfgroup.org>"

COPY --from=hsds-base /opt/env/hsds /opt/env/hsds

# Install HSDS
RUN mkdir /usr/local/src/hsds-src/ \
          /usr/local/src/hsds-src/hsds/ \
          /usr/local/src/hsds-src/admin/ \
          /usr/local/src/hsds-src/admin/config/ \
          /usr/local/src/hsds-src/hsds/util/ \
           /usr/local/src/hsds/ \
           /etc/hsds/ 

COPY setup.py /usr/local/src/hsds-src
COPY hsds/*.py /usr/local/src/hsds-src/hsds
COPY hsds/util/*.py /usr/local/src/hsds-src/hsds/util
COPY admin/config/config.yml /etc/hsds/
COPY admin/config/config.yml /usr/local/src/hsds-src/admin/config/
COPY entrypoint.sh  /
RUN /bin/bash -c "source /opt/env/hsds/bin/activate \
    && pip install /usr/local/src/hsds-src/ \
    && rm -rf /usr/local/src/hsds-src"

EXPOSE 5100-5999
ENTRYPOINT ["/bin/bash", "-c", "source /opt/env/hsds/bin/activate && /entrypoint.sh"]
