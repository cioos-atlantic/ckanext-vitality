.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://travis-ci.org/cioos-atlantic/ckanext-vitality_prototype.svg?branch=master
    :target: https://travis-ci.org/cioos-atlantic/ckanext-vitality_prototype

.. image:: https://coveralls.io/repos/cioos-atlantic/ckanext-vitality_prototype/badge.svg
  :target: https://coveralls.io/r/cioos-atlantic/ckanext-vitality_prototype

.. image:: https://pypip.in/download/ckanext-vitality_prototype/badge.svg
    :target: https://pypi.python.org/pypi//ckanext-vitality_prototype/
    :alt: Downloads

.. image:: https://pypip.in/version/ckanext-vitality_prototype/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-vitality_prototype/
    :alt: Latest Version

.. image:: https://pypip.in/py_versions/ckanext-vitality_prototype/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-vitality_prototype/
    :alt: Supported Python versions

.. image:: https://pypip.in/status/ckanext-vitality_prototype/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-vitality_prototype/
    :alt: Development Status

.. image:: https://pypip.in/license/ckanext-vitality_prototype/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-vitality_prototype/
    :alt: License

=============
ckanext-vitality_prototype
=============

.. Put a description of your extension here:
   What does it do? What features does it have?
   Consider including some screenshots or embedding a video!


------------
Requirements
------------

Developed for the CIOOS CKAN fork (https://github.com/cioos-siooc/ckan).


------------
Installation
------------

.. Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-vitality_prototype:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-vitality_prototype Python package into your virtual environment::

     pip install ckanext-vitality_prototype

3. Add ``vitality_prototype`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload

5. Launch a local instance of Neo4J as a docker container

     docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=none neo4j:3.5.8

6. Seed the CKAN users into the metadata authorization model

     docker exec ckan /usr/local/bin/ckan-paster --plugin=ckanext-vitality_prototype vitality seed --config=/etc/ckan/production.ini

7. Re-index the datasets in your CKAN instance

    This will add them to the authorization model

     docker exec ckan /usr/local/bin/ckan-paster --plugin=ckan search-index rebuild --config=/etc/ckan/production.ini




---------------
Config Settings
---------------

Document any optional config settings here. For example::

    # The minimum number of hours to wait before re-checking a resource
    # (optional, default: 24).
    ckanext.vitality_prototype.some_setting = some_default_value


------------------------
Development Installation
------------------------

To install ckanext-vitality_prototype for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/cioos-atlantic/ckanext-vitality_prototype.git
    cd ckanext-vitality_prototype
    python setup.py develop
    pip install -r dev-requirements.txt


-----------------
Running the Tests
-----------------

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.vitality_prototype --cover-inclusive --cover-erase --cover-tests


---------------------------------
Registering ckanext-vitality_prototype on PyPI
---------------------------------

ckanext-vitality_prototype should be availabe on PyPI as
https://pypi.python.org/pypi/ckanext-vitality_prototype. If that link doesn't work, then
you can register the project on PyPI for the first time by following these
steps:

1. Create a source distribution of the project::

     python setup.py sdist

2. Register the project::

     python setup.py register

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the first release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.1 then do::

       git tag 0.0.1
       git push --tags


----------------------------------------
Releasing a New Version of ckanext-vitality_prototype
----------------------------------------

ckanext-vitality_prototype is availabe on PyPI as https://pypi.python.org/pypi/ckanext-vitality_prototype.
To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Create a source distribution of the new version::

     python setup.py sdist

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.2 then do::

       git tag 0.0.2
       git push --tags
