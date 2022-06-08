
FROM python:3.10-slim as base

# Here, build the python environment for the deployment
FROM base as build
# Install pipenv
RUN pip install pipenv
# Install  build and compilation dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential
RUN mkdir /app \
    /app/hsds \
    /app/cp
COPY Pipfile /app/Pipfile
COPY Pipfile.lock /app/Pipfile.lock
COPY requirements.txt /app/requirements.txt
COPY setup.py /app/setup.py
COPY hsds /app/hsds/
COPY admin /app/admin/
COPY entrypoint.sh  /
WORKDIR /app
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
RUN apt-get remove -y build-essential
ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]
