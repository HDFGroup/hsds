FROM python:3.9
LABEL MAINTAINER="John Readey, The HDF Group"
ENV HS_ENDPOINT=
RUN pip install numpy
RUN pip install git+https://github.com/HDFGroup/h5pyd
RUN mkdir /app
COPY write_hdf.py /app
COPY entrypoint.sh /
WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
