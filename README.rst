GCP Python Client Library for Data Science
==========================

    GCP-DSClient (`Google Cloud Platform`_ Data Science Client)
    helps interactive data science by coordinating data science libraries (pandas, ipython) and Google Cloud Infrastructure.

.. _Google Cloud Platform: https://cloud.google.com/

This client supports the following Google Cloud Platform services:

-  `Google BigQuery`_
-  `Google Cloud Storage`_

.. _Google BigQuery: https://github.com/orfeon/gcp-python-dsclient#google-bigquery
.. _Google Cloud Storage: https://github.com/orfeon/gcp-python-dsclient#google-cloud-storage

Quick Start
-----------

Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    $ pip install --upgrade gcp-dsclient


Initialize Client instance
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import dsclient

    # When run on GCE instance that has GCP access permission,
    # you need to set only project name.
    client = dsclient.Client("your project name")

    # In other case (ex: run on local PC),
    # you need to set project name, and access key file path.
    client = dsclient.Client("your project name", "./keyfile.json")

Usage Google BigQuery with Pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~

Google `BigQuery`_ (`BigQuery API docs`_)

.. _BigQuery: https://cloud.google.com/storage/docs
.. _BigQuery API docs: https://cloud.google.com/storage/docs/json_api/v1

.. code:: python

    # Query and read data as pandas.DataFrame
    query_string = """
        SELECT date, year
        FROM aaa
        WHERE year = 2016
    """
    df = client.query(query_string) # Use lquery() for large data.

    # Upload(Append) pandas.DataFrame to existing table on BigQuery.
    client.load(df, "your_dataset.your_table")
    # Override existing table.
    client.load(df, "your_dataset.your_table", append=False)

    # Insert query result into table. (Override if table exists)
    client.insert(query_string, "your_dataset.your_table_2")


Usage Google Cloud Storage with Pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~

Google `Cloud Storage`_ (`Storage API docs`_)

.. _Cloud Storage: https://cloud.google.com/storage/docs
.. _Storage API docs: https://cloud.google.com/storage/docs/json_api/v1

`official Google Cloud Storage documentation`_

.. _official Google Cloud Storage documentation: https://cloud.google.com/storage/docs/cloud-console#_creatingbuckets

.. code:: python

    import pandas as pd

    # Write local pandas.DataFrame to Cloud Storage.
    df1 = pd.DataFrame(...somedata...)
    client.write_csv(df1, "gs://your_bucket/your_file1.csv")

    # Read pandas.DataFrame from csv file on Cloud Storage.
    df2 = client.read_csv("gs://your_bucket/your_file2.csv")

    # Write blob data (ex: ML model) to Cloud Storage.
    reg = LinearRegressor()
    reg.fit(df1[["attr1","attr2",...]], df1["target"])
    client.write_blob(reg, "gs://your_bucket/your_file.model")

    # Read blob data from Cloud Storage.
    reg = client.read_blob("gs://your_bucket/your_file.model")
    prd = reg.predict(df2[["attr1","attr2",...]])


Usage Google Compute Engine with IPython and IPyParallel
~~~~~~~~~~~~~~~~~~~~~~~~~~

    .. code:: python

    # create snapshot of current instance on GCE.
    client.create_current_snapshot(snapshot_name)

    # deploy ipcluster from snapshot.
    client.deploy_ipcluster(profile="mycluster", snapshot=snapshot_name,
                            itype="standard", core=4, num=4)

    # execute some tasks on ipcluster.

    # add ipengine to existing ipcluster.
    client.add_ipengine(profile="mycluster", snapshot=snapshot_name,
                        itype="small", num=4)

    # delete ipcluster.
    client.delete_ipcluster(profile="mycluster")

    # stop current instance when all tasks are finished.
    client.stop_current_instance()


License
-------

Apache 2.0 - See `LICENSE`_ for more information.

.. _LICENSE: https://github.com/orfeon/gcp-python-dsclient/blob/master/LICENSE
