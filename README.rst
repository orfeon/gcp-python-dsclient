Google Cloud Platform Python Client for Data Science
==========================

    Simple Python wrapper client library for interactive data science with `Google Cloud Platform`_ services.

.. _Google Cloud Platform: https://cloud.google.com/

This client supports the following Google Cloud Platform services:

-  `Google BigQuery`_
-  `Google Cloud Storage`_
-  `Google Cloud Datastore`_

.. _Google BigQuery: https://github.com/orfeon/gcp-python-dsclient#google-bigquery
.. _Google Cloud Storage: https://github.com/orfeon/gcp-python-dsclient#google-cloud-storage
.. _Google Cloud Datastore: https://github.com/orfeon/gcp-python-dsclient#google-cloud-datastore

Quick Start
-----------

::

    $ pip install --upgrade gcp-dsclient

Google Cloud Storage
--------------------

Google `Cloud Storage`_ (`Storage API docs`_) allows you to store data on Google
infrastructure with very high reliability, performance and availability, and can
be used to distribute large data objects to users via direct download.

.. _Cloud Storage: https://cloud.google.com/storage/docs
.. _Storage API docs: https://cloud.google.com/storage/docs/json_api/v1

You need to create a Google Cloud Storage bucket to use this client library.
Follow along with the `official Google Cloud Storage documentation`_ to learn
how to create a bucket.

.. _official Google Cloud Storage documentation: https://cloud.google.com/storage/docs/cloud-console#_creatingbuckets

Upload CSV file to CloudStorage from local pandas.DataFrame
~~~~~~~~~~~~~~~~~~

.. code:: python

    import pandas as pd
    from dsclient import storage

    client = storage.Client()

    df = pd.DataFrame(...somedata...)
    client.write_csv(bucket_name, file_path, df)


License
-------

Apache 2.0 - See `LICENSE`_ for more information.

.. _LICENSE: https://github.com/orfeon/gcp-python-dsclient/blob/master/LICENSE
