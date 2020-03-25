# Build image for packages that need compilation.
FROM hdfgroup/python:3.7 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y gcc krb5-user libkrb5-dev
RUN pip wheel -w /opt/wheels gssapi

# Production image for runtime.
FROM hdfgroup/python:3.7 AS runtime
MAINTAINER John Readey <jreadey@hdfgroup.org>

# Install krb5 libs and gssapi package.
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user
COPY --from=build /opt/wheels/* /opt/wheels/
RUN pip install /opt/wheels/*

# Install python packages from pip.
RUN pip install azure-storage-blob
RUN pip install aiofiles
RUN pip install pyjwt

# Copy config files.
RUN mkdir /usr/local/src/hsds/ /usr/local/src/tests/
COPY hsds /usr/local/src/hsds/
COPY admin/config/krb5.conf /etc/krb5.conf
COPY admin/config/passwd.txt /usr/local/src/hsds/
COPY tests /usr/local/src/tests/
COPY testall.py /usr/local/src/
COPY entrypoint.sh  /

EXPOSE 5100-5999

ENTRYPOINT ["/entrypoint.sh"]
