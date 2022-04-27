"""
Tests for graph_meta_authorize.py.
Note for unittest may want to separate the different tests into classes & functions
rather than keeping all within one
Can use -v on run to return verbose tests with more detail

testDict is used to simulate pkg_dict throughout the file
Using data directly from the model is avoided to narrow down cases 
    if something doesn't match the tests, but perhaps some proper
    mock data should be used here at some point in the future
"""

import unittest
from ckanext.vitality_prototype.meta_authorize import MetaAuthorize, MetaAuthorizeType

class TestNeo4j(unittest.TestCase):
    testAuthorize = MetaAuthorize.create(MetaAuthorizeType.GRAPH, {
            'host': "bolt://localhost:7476",
            'user': "neo4j",
            'password': "password"
        })

    def test_createData(self):
        self.testAuthorize.add_user("test", "test")
        self.assertTrue(True)

# Required to run unit test
if __name__ == '__main__':
    unittest.main()


