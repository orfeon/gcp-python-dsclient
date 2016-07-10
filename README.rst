Python Client Library for Data Science on GCP
==========================

    GCP-DSClient (`Google Cloud Platform`_ Client library for Data Science)
    helps interactive data science using famous data science libraries such as pandas, ipython on GCP.

.. _Google Cloud Platform: https://cloud.google.com/

This client supports the following Google Cloud Platform services:

-  `Google BigQuery`_
-  `Google Cloud Storage`_

.. _Google BigQuery: https://github.com/orfeon/gcp-python-dsclient#google-bigquery
.. _Google Cloud Storage: https://github.com/orfeon/gcp-python-dsclient#google-cloud-storage

Quick Start
-----------

::

    $ pip install --upgrade gcp-dsclient


Initialize Client instance
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from dsclient import gcp

    # When run on GCE instance that has GCP access permission,
    # you need to set only project name.
    client = gcp.Client("your project name")

    # In other case (ex: run on local PC),
    # you need to set project name, service email, and access key file path.
    client = gcp.Client("your project name", "xxxxxxx@gmail.com", "./keyfile.p12")

Usage for Google BigQuery
--------------------

Google `BigQuery`_ (`BigQuery API docs`_)

.. _Cloud Storage: https://cloud.google.com/storage/docs
.. _Storage API docs: https://cloud.google.com/storage/docs/json_api/v1

.. code:: python

    import pandas as pd

    # Query table and read data as pandas.DataFrame
    query_string = """
        SELECT date, year
        FROM aaa
        WHERE year = 2016
    """
    df = client.query(query_string)
    # If query large data, use client.lquery()

    # Upload pandas.DataFrame to table on BigQuery.
    client.load(df, "your_dataset.your_table", append=True)


Usage for Google Cloud Storage
--------------------

Google `Cloud Storage`_ (`Storage API docs`_)

.. _Cloud Storage: https://cloud.google.com/storage/docs
.. _Storage API docs: https://cloud.google.com/storage/docs/json_api/v1

`official Google Cloud Storage documentation`_

.. _official Google Cloud Storage documentation: https://cloud.google.com/storage/docs/cloud-console#_creatingbuckets

Write/Read CSV file to/from Cloud Storage from/to local pandas.DataFrame
~~~~~~~~~~~~~~~~~~

.. code:: python

    import pandas as pd

    # you can write local pandas.DataFrame to Cloud Storage.
    df1 = pd.DataFrame(...somedata...)
    client.write_csv(df1, "gs://your_bucket_name/your_file_path.csv")

    # you can read pandas.DataFrame from csv file on Cloud Storage.
    df2 = client.read_csv("gs://your_bucket_name/your_file_path.csv")

License
-------

Apache 2.0 - See `LICENSE`_ for more information.

.. _LICENSE: https://github.com/orfeon/gcp-python-dsclient/blob/master/LICENSE
