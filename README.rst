=================================
VSC Python iRODS Client (VSC-PRC)
=================================

Welcome to the Vlaams Supercomputing Centrum (VSC_) extensions to the
Python iRODS Client (PRC_)!

VSC-PRC's main goal is to make it easier for researchers to manage their data
using iRODS_, in particular on VSC's high performance computing infrastructure.
To this end, VSC-PRC offers a Python module and associated command line scripts.


Dependencies
============

* Python3
* python-irodsclient


Installation
============

* Clone or download from GitHub
* Set the `$VSC_PRC_ROOT` environment variable to the location of VSC-PRC's
  root directory
* Add the `$VSC_PRC_ROOT/lib` directory to your `$PYTHONPATH`
* Add the `$VSC_PRC_ROOT/tools` directory to your `$PATH`
* Run the tests in the `$VSC_PRC_ROOT/test` folder


On VSC's BrENIAC cluster, VSC-PRC and its dependencies are also available
as a module:

.. code:: bash

    module use /apps/leuven/common/modules/all
    module load vsc-python-irodsclient/development

.. _VSC: https://vscentrum.be
.. _PRC: https://github.com/irods/python-irodsclient
.. _iRODS: https://irods.org
