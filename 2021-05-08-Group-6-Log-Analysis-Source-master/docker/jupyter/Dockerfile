FROM jupyter/datascience-notebook:python-3.8.6

# Add provisioning
ADD ./notebooks jupyter/
ADD ./notebooks/data jupyter/data/
RUN pip install pyarrow