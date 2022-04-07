FROM python:3.9
LABEL MAINTAINER="John Readey, The HDF Group"
ENV HS_ENDPOINT=
RUN pip install numpy
RUN pip install h5pyd
RUN mkdir /app
COPY hs_write.py /app
COPY entrypoint.sh /
WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
