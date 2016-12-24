docker run -d --name notebook --user root -p 5901:8888 -e GRANT_SUDO=yes -v C:\Users\yoich\Documents\Projects\gcp-python-dsclient:/home/jovyan/work jupyter/datascience-notebook
