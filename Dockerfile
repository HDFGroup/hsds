
FROM python:3.10 as base

# Here, build the python environment for the deployment
FROM base as build
# Install pipenv
RUN pip install pipenv
# Install  build and compilation dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc
RUN mkdir /app \
    /app/hsds \
    /app/cp
COPY Pipfile /app/Pipfile
COPY Pipfile.lock /app/Pipfile.lock
COPY requirements.txt /app/requirements.txt
COPY setup.py /app/setup.py
COPY hsds /app/hsds
COPY admin /app/admin
WORKDIR /app
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]
