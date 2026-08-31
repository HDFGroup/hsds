FROM mcr.microsoft.com/devcontainers/miniconda:0-3

# commenting out mamba install, is given an error, see: 
# https://github.com/conda/conda-libmamba-solver/issues/540
RUN conda install -n base -c conda-forge

# Copy environment.yml (if found) to a temp location so we update the environment. Also
# copy "noop.txt" so the COPY instruction does not fail if no environment.yml exists.
COPY environment.yml* .devcontainer/noop.txt /tmp/conda-tmp/
RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/mamba env create -f /tmp/conda-tmp/environment.yml; fi \
    && rm -rf /tmp/conda-tmp

RUN conda install -n base ipykernel requests
