FROM jupyter/datascience-notebook
USER root
RUN set -x && \
    apt-get update -y && \
    apt-get install cmake -y && \
    apt-get install python-numpy -y
RUN /opt/conda/envs/python2/bin/python setup.py install
