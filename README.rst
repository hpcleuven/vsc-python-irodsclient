=================================
VSC Python iRODS Client (VSC-PRC)
=================================

`Documentation on ReadTheDocs`_

Welcome to the Vlaams Supercomputing Centrum (VSC_) extensions to the
Python iRODS Client (PRC_)!

VSC-PRC's main goal is to make it easier for researchers to manage their data
using iRODS_, in particular on VSC's high performance computing infrastructure.

To this end, VSC-PRC offers a Python module and associated command line scripts:

* The :code:`vsc_irods` Python module contains a :code:`VSCiRODSSession` class
  which represents an extension of the corresponding :code:`iRODSSession` class
  in PRC.

  A main feature is the possibility of using wildcards ("*") and tildes
  ("~") for specifying iRODS data objects and collections. For example,
  the following code will copy all files ending on '.txt' inside a
  'my_irods_collection' collection in your irods_home to the local working
  directory:

  .. code:: python

    >>> from vsc_irods.session import VSCiRODSSession
    >>>
    >>> with VSCiRODSSession() as session:
    >>>     session.bulk.get('~/my_irods_collection/*.txt', local_path='.')

  Other 'bulk' operations are available for:

  - uploading files and folders
  - removing data objects and collections
  - adding and modifying metadata
  - listing the disk usage

  More advanced search capabilities (i.e. beyond the above glob patterns)
  are also provided. For example, the following can be used to list all
  data objects in your irods_home ending on '.txt' and which possess a
  metadata entry with Attribute='Author' and Value='Me':

  .. code:: python

    >>> with VSCiRODSSession() as session:
    >>>     for item in session.search.find('~', pattern='*.txt', types='f',
    >>>                                     object_avu=('Author', 'Me')):
    >>>         print(item)

  This can be used in conjunction with the 'bulk' operations, e.g.:

  .. code:: python

    >>> with VSCiRODSSession() as session:
    >>>     iterator = session.search.find('~', pattern='*.txt', types='f',
    >>>                                    object_avu=('Author', 'Me'))
    >>>     session.bulk.get(iterator, local_path='.')


* VSC-PRC also comes with a set of scripts which make it easy to use the
  Python module from a Unix shell:

  - vsc-prc-find
  - vsc-prc-iget
  - vsc-prc-iput
  - vsc-prc-imkdir
  - vsc-prc-irm
  - vsc-prc-size
  - vsc-prc-imeta
  - vsc-prc-add-job-metadata

  Typing e.g. :code:`vsc-prc-find --help` will show a description of the
  recognized arguments. The command-line equivalents of the three Python
  snippets above, for example, would look like this:

  .. code:: bash

    vsc-prc-iget '~/my_irods_collection/*.txt' -d .
    vsc-prc-find '~' -n '*.txt' --object_avu='Author;Me'
    vsc-prc-find '~' -n '*.txt' --object_avu='Author;Me' | xargs -i vsc-prc-iget {} -d .

More examples can be found in the :code:`examples` directory.


Dependencies
============

* Python3
* python-irodsclient >= v0.8.4


Installation
============

* Clone or download from GitHub
* Set the :code:`$VSC_PRC_ROOT` environment variable to the location of
  VSC-PRC's root directory
* Add the :code:`$VSC_PRC_ROOT/lib` directory to your :code:`$PYTHONPATH`
* Add the :code:`$VSC_PRC_ROOT/tools` directory to your :code:`$PATH`
* Run the tests in the :code:`$VSC_PRC_ROOT/test` folder

Note: the last test in test.py assumes the iCAT database is case-sensitive.
If this is not the case for your environment, feel free to comment out this test.


On VSC's BrENIAC cluster, VSC-PRC and its dependencies are also available
as a module:

.. code:: bash

    module load vsc-python-irodsclient/development

.. _VSC: https://vscentrum.be
.. _PRC: https://github.com/irods/python-irodsclient
.. _iRODS: https://irods.org
.. _Documentation on ReadTheDocs: https://vsc-python-irodsclient.readthedocs.io
