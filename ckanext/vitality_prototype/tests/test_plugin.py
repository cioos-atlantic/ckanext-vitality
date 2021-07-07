"""Tests for plugin.py."""
from uuid import uuid4
import ckanext.vitality_prototype.plugin as plugin

testClass = plugin.Vitality_PrototypePlugin()

def test_plugin():
    pass

def test_compute_tabs():
    teststring = plugin.computeTabs(2)
    expected = "\t\t" 
    assert(teststring == expected)

def test_default_keys():
    testDict = {'id': "An identifier", 'not_a_default_field': "not_default_value"}
    testCase = plugin.default_public_fields(testDict)
    assert(testDict.keys == ['id'])

def test_generate_default_fields():
    default_fields = plugin.generate_default_fields()
    sample_default_keys = ['id', 'state', 'tags', 'unique-resource-identifier-full', 'relationships-as-subject']
    assert(sample_default_keys.issubset(default_fields.keys()))
    assert(default_fields.all(lambda x : x is uuid4))