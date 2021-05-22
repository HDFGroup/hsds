FROM continuumio/miniconda3@sha256:7838d0ce65783b0d944c19d193e2e6232196bada9e5f3762dc7a9f07dc271179 AS hsds-base
LABEL maintainer="Aleksandar Jelenak <help@hdfgroup.org>"
RUN conda update conda -y && \
    conda config --add channels conda-forge && \
    conda config --set channel_priority strict && \
    conda install conda-pack && \
    conda create --name hsds --yes python=3.8
RUN conda install --name hsds --yes \
        curl \
        git \
        compilers \
        psutil \
        numpy \
        pytz \
        requests \
        aiobotocore \
        azure-storage-blob \
        aiofiles \
        aiohttp \
        aiohttp-cors \
        pyjwt \
        pyyaml \
        pip \
        simplejson \
        wheel
# Install numcodecs from the specific commit since we need the brand new shuffle codec...
RUN DISABLE_NUMCODECS_AVX2=1 CFLAGS=-DHAVE_UNISTD_H \
    conda run -n hsds --no-capture-output pip install --no-cache-dir \
    git+https://github.com/zarr-developers/numcodecs.git@d16d1eac5198166a24726ffe808e3dcfcab9700d#egg=numcodecs \
    && conda remove --name hsds --yes git compilers \
    && conda run -n hsds --no-capture-output pip install --no-cache-dir kubernetes
RUN conda-pack -n hsds -o /tmp/hsds-env.tar \
    && mkdir -p /opt/env/hsds \
    && cd /opt/env/hsds \
    && tar xf /tmp/hsds-env.tar \
    && rm /tmp/hsds-env.tar
RUN /opt/env/hsds/bin/conda-unpack

#
#
#

FROM debian:buster-slim AS hsds
LABEL maintainer="Aleksandar Jelenak <help@hdfgroup.org>"

COPY --from=hsds-base /opt/env/hsds /opt/env/hsds

# Install HSDS
RUN mkdir /usr/local/src/hsds-src/ /usr/local/src/hsds/ /etc/hsds/
COPY . /usr/local/src/hsds-src
COPY admin/config/config.yml /etc/hsds/
COPY entrypoint.sh  /
RUN /bin/bash -c "source /opt/env/hsds/bin/activate \
    && pip install /usr/local/src/hsds-src/ --no-deps \
    && rm -rf /usr/local/src/hsds-src"

EXPOSE 5100-5999
ENTRYPOINT ["/bin/bash", "-c", "source /opt/env/hsds/bin/activate && /entrypoint.sh"]
