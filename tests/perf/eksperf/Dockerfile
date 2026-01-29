FROM python:3.11.9
RUN pip install git+https://github.com/hdfgroup/h5pyd.git
RUN pip install pyyaml
RUN pip install h5py
RUN pip install s3fs
COPY entrypoint.sh /
RUN chmod 555 /entrypoint.sh
RUN mkdir /app
COPY read_shots.py /app
COPY config.py /app
COPY config.yml /app
WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]