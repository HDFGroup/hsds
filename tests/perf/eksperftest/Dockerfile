FROM hdfgroup/hdf5lib:1.14.4
LABEL MAINTAINER="John Readey, The HDF Group"
ENV RUN_COMMAND=
RUN pip install tzlocal
RUN pip install pyyaml
RUN mkdir /app
COPY cmod_read.py /app
COPY dump_inventory.py /app
COPY make_inventory.py /app
COPY config.py /app
COPY entrypoint.sh  /
WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
