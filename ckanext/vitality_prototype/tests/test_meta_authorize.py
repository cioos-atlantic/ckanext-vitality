"""
Tests for meta_authorize.py.
Note for unittest may want to separate the different tests into classes & functions
rather than keeping all within one
Can use -v on run to return verbose tests with more detail

testDict is used to simulate pkg_dict throughout the file

"""
import unittest
import ckanext.vitality_prototype.meta_authorize as meta_authorize
from ckan.common import config
from uuid import uuid4

# Testing the create function
# TODO write create tests (not implemented yet)
class TestCreate(unittest.TestCase):

    testClass_create = meta_authorize.MetaAuthorize()

    def test_create_simple(self):
        opts= {}
        #self.testClass_create.create(meta_authorize.MetaAuthorizeType.SIMPLE, opts)
        pass

    def test_create_graph(self):
        #TODO Get from preferences instead of hard coded
        opts=  {
            'host': "bolt://192.168.2.38:7687",
            'user': "neo4j",
            'password': "password"
        }
        #self.testClass_create =meta_authorize.MetaAuthorize.create(meta_authorize.MetaAuthorizeType.GRAPH, opts)
        pass

# Testing the decode function
class TestDecode(unittest.TestCase):
    """
    Runs testing methods related to the _decode method in meta_authorize
    """

    testClass_decode = meta_authorize.MetaAuthorize()


    def test_decode_empty(self):
        """
        Tests decode function with an empty dict
        Expected outcome is an empty dict
        """
        empty_dict= {}
        self.assertDictEqual(self.testClass_decode._decode(empty_dict), {})

    def test_decode_wrongType(self):
        """
        Tests decode if given an improper type
        Expected outcome is a TypeError
        """
        with self.assertRaises(TypeError):
            self.testClass_decode._decode(1)
        with self.assertRaises(TypeError):
            self.testClass_decode._decode(["test"])
        with self.assertRaises(TypeError):
            self.testClass_decode._decode({1})

    def test_decode_encodedStr_outputUnicode(self):
        """
        Test decode to ensure that string values are output as unicode
        Expected output: bbox-north-lat value is unicode
        """
        testStr= "{\"bbox-north-lat\": \"50.23343445\"}"
        testDict = self.testClass_decode._decode(testStr)
        self.assertIsInstance(testDict, dict)
        self.assertIsInstance(testDict['bbox-north-lat'], unicode)

    def test_decode_encodedStr_wDict(self):
        """
        Tests decode by inputting an encoded String and see if it returns a managable dict
        Expected outcome is the data from the string, but in a dict with a nested dict
        Keys and values are in unicode format
        """
        testStr= "{\"metadata-point-of-contact\": {\"contact-info_email\":\"testemail@test.ca\", \"individual-name\":\"testgroup\"}}"
        testDict = self.testClass_decode._decode(testStr)
        compareDict = {
            u'metadata-point-of-contact': {
                u'contact-info_email' : u'testemail@test.ca',
                u'individual-name' : u'testgroup'
            }
        }
        self.assertIsInstance(testDict, dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_encodedStr_wNestedList(self):
        """
        Tests decode by inputting a JSON String with an array
        Expected outcome is the data from the string, but in a dict with a nested list
        Keys and values are in unicode format
        """
        testStr= "{\"cited-responsible-party\": [{\"role\":\"author\", \"organisation-name\":\"testorg\"}]}"
        testDict = self.testClass_decode._decode(testStr)
        compareDict = {
            u'cited-responsible-party':[
                {
                    u'role':u'author', 
                    u'organisation-name':u'testorg'
                }
            ]
        }
        self.assertIsInstance(testDict, dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_dict(self):
        """
        Inputs a simple dictionary into decode to ensure nothing breaks
        Expected output: return the same dict
        """
        testDict = {
            "id": "986ce776-e6d7-484d-9b7c-3449a7368649",
            "name": "testdataset",
            "private": False,
            "num_resources": 1
        }
        self.assertDictEqual(self.testClass_decode._decode(testDict), testDict)

    def test_decode_dict_nestedDict(self):
        """
        Test decode by inputting a dict that contains another dict, 
            ensure the recursion works and doesn't break anything
        """
        testDict = {
            "temporal-extent": {
                "begin":"2014-09-02", 
                "end":"2018-01-02", 
                "type":{
                    "concurrent": True
                }
            }
        }
        compareDict = {
            u'temporal-extent': {
                u'begin':u'2014-09-02', 
                u'end':u'2018-01-02', 
                u'type':{
                    u'concurrent': True
                }
            }
        }
        testDict = self.testClass_decode._decode(testDict)
        self.assertIsInstance(testDict, dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_dict_wEncodedDict(self):
        """
        Test decode with a dict containing a basic JSON String to ensure it is parsed to a dict
        Expected outcome is metadata-point-of-contact value to become a dict
        """
        testDict = {
            'metadata-point-of-contact': "{\"organisation-name\": \"CIOOS\",\"role\": \"author\"}"
        }
        testDict = self.testClass_decode._decode(testDict)
        compareDict = {
            'metadata-point-of-contact': {
                'organisation-name': "CIOOS",
                'role': "author"
            }
        }
        self.assertIsInstance(testDict, dict)
        self.assertIsInstance(testDict['metadata-point-of-contact'], dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_dict_wEncodedNestedDict(self):
        """
        Test decode with a dict containing a basic JSON String to ensure it is parsed to a dict
        Expected outcome is metadata-point-of-contact value to become a dict
        """
        testDict = {
            "resources": "{\"state\": \"active\",\"tracking_summary\":{\"total\": 0, \"recent\": 0}}"
        }
        compareDict = {
            "resources": {
                'state': 'active',
                'tracking_summary':{
                    'total': 0, 
                    'recent': 0
                }
            }
        }
        testDict = self.testClass_decode._decode(testDict)
        self.assertIsInstance(testDict, dict)
        self.assertIsInstance(testDict['resources'], dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_dict_wEncodedList(self):
        """
        Test with a dict containing a string encoded JSON list
        Expected outcome is to have datasete reference date decode into a list
            containing dict items
        """
        testDict = {
            "dataset-reference-date":"[{\"type\":\"creation\",\"value\":\"2014-09-02\"},{\"type\":\"revision\", \"value\":\"2018-01-02\"}]"
        }
        compareDict = {
            u'dataset-reference-date':[{
                u'type':u'creation',
                u'value':u'2014-09-02'
            },{
                u'type':u'revision',
                u'value':u'2018-01-02'
            }]
        }
        testDict = self.testClass_decode._decode(testDict)
        self.assertIsInstance(testDict, dict)
        self.assertIsInstance(testDict['dataset-reference-date'], list)
        self.assertIsInstance(testDict['dataset-reference-date'][0], dict)
        self.assertDictEqual(testDict, compareDict)

    def test_decode_dict_wEncodedDictAndList(self):
        """
        Test with a dict containing string encoded list and dict
        Expected outcome is to temporal extent to decode into a dict and
            dataset reference date decode into a list containing dict items
        """
        testDict = {
            "temporal-extent":"{\"begin\":\"2020-01-05\", \"end\":\"2020-12-06\"}",
            "dataset-reference-date":"[{\"type\":\"creation\",\"value\":\"2014-09-02\"},{\"type\":\"revision\", \"value\":\"2018-01-02\"}]"
        }
        compareDict = {
            u'temporal-extent':{
                u'begin': u'2020-01-05',
                u'end':u'2020-12-06'
            },
            u'dataset-reference-date':[{
                u'type':u'creation',
                u'value':u'2014-09-02'
            },{
                u'type':u'revision',
                u'value':u'2018-01-02'
            }]

        }
        testDict = self.testClass_decode._decode(testDict)
        self.assertIsInstance(testDict, dict)
        self.assertDictEqual(testDict, compareDict)

    # Test with a sample of pkgDict
    # TODO still need to finish this one
    def test_decode_sample_pkgDict(self):
        pass
    
# Testing the encode function
class TestEncode(unittest.TestCase):

    testClass_encode = meta_authorize.MetaAuthorize()

    def test_encode_wDict_notStringified(self):
        """
        Tests encoding a dict object that is not listed in 
            Stringified fields for constants
        Expected output is no change
        """
        testDict = {
            'id': '986ce776-e6d7-484d-9b7c-3449a7368649',
            'name': 'testdataset',
            'private': False,
            'num_resources': 1
        }
        encoded = self.testClass_encode._encode(testDict)
        self.assertIsInstance(encoded, dict)
        self.assertDictEqual(encoded, testDict)

    def test_encode_wDict_inStringified(self):
        """
        Tests encoding a dict object that is listed in 
            Stringified fields for constants
        Expected output is to create stringified version of dict
        """
        testDict = {
            'temporal-extent' : {
                "begin": "2014-09-02", 
                "end": "2018-01-02"
            }
        }
        compareDict = {
            'temporal-extent': u'{"begin": "2014-09-02", "end": "2018-01-02"}'
        }
        encoded = self.testClass_encode._encode(testDict)
        self.assertDictEqual(encoded, compareDict)

    def test_encode_wList_notStringified(self):
        """
        Tests encoding a list object (within a dict) that is not listed in 
            Stringified fields for constants
        Expected output is no change
        """
        testDict = {
            'resources': [{'state': 'active','tracking_summary':{'total': 0, 'recent': 0}}]
        }
        encoded = self.testClass_encode._encode(testDict)
        self.assertDictEqual(encoded, testDict)
        
    def test_encode_wList_inStringified(self):
        """
        Tests encoding a list object that is listed in 
            Stringified fields for constants
        Expected output is to create stringified version of list
        """
        testDict= {
            u'dataset-reference-date': [{"type": "creation", "value": "2014-09-02"}, {"type": "revision", "value": "2018-01-02"}]
        }
        compareDict= {
            u'dataset-reference-date': u'[{"type": "creation", "value": "2014-09-02"}, {"type": "revision", "value": "2018-01-02"}]' 
        }
        encoded = self.testClass_encode._encode(testDict)
        self.assertDictEqual(encoded, compareDict)
        
# Testing the filter_dict function
class TestFilterDict(unittest.TestCase):
    test_filterDict = meta_authorize.MetaAuthorize()

    testDict = {
        u'bbox-north-lat': u'50.23343445',
        u'bbox-south-lat': u'52.33428394',
        u'bbox-east-long': u'-54.9435608299',
        u'bbox-west-long': u'-54.9243032933',
        u'num_resources': 1,
        u'num_tags': 17,
        u'tracking_summary':{'total':6, 'recent':6},
        u'temporal-extent': u'{"begin": "2014-09-02", "end": "2018-01-02"}',
        u'resources': [{'state': 'active','tracking_summary':{'total': 0, 'recent': 0}}],
        u'dataset-reference-date': u'[{"type": "creation", "value": "2014-09-02"},{"type": "revision","value": "2018-01-02"}]'
    }
    #TODO Generate these upon testing instead of hardcoding
    testFields = {
        'temporal-extent':'e6f6cd2f-ae22-4a01-9c9c-73da48dc2e9a',
        'temporal-extent/begin': '63ab7274-fe8c-457c-8162-58ee614dafd8',
        'temporal-extent/end': '1b72740e-e314-4278-ba3c-9a38f8433fe8',
        'resources': '68661db8-de9b-4773-847d-1dcd73106cb1',
        'resources/state': '9c76371d-1605-4498-bddb-570c752c9eeb',
        'resources/tracking_summary': 'a1c61940-59a8-4322-8ae6-c45d63bd3576',
        'resources/tracking_summary/total': '9064eeaa-4df8-4317-821e-deb9656c2d76',
        'resources/tracking_summary/recent': '3da6cbe6-0e89-44c4-8b9b-de068927339c',
        'tracking_summary':'f25104ef-3b01-48f7-bbf0-5755cbea40da',
        'tracking_summary/total':'f028aef9-951c-44bb-906d-f3d50e8d1782',
        'tracking_summary/recent':'550dd97c-2462-4106-a126-12764ad243e3',
        'num_resources': '0964c7f8-f601-4b48-892b-6898b0bf42a5',
        'num_tags': 'e3a743d7-7108-4101-96b0-91750dbc151f',
        'bbox-north-lat': '8dfa8f2b-4e13-48c1-8a49-d706ecfae5f2',
        'bbox-south-lat': 'c124c58b-1723-4d7d-908e-772ef7713950',
        'bbox-east-long': 'edcb7683-79a8-4b0d-8524-a8c0d313f932',
        'bbox-west-long': 'd82134c5-99ce-467a-a315-73b71def75d2',
        'dataset-reference-date':'ee2c9a3b-865d-4aa8-a251-dca6a6b7427d'  
    }

    def test_filter_noWhitelist(self):
        """
        Test filter_dict to see if leaving everything off the whitelist excludes them
        Expected outcome is an empty dict
        """
        whitelist= []
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        self.assertDictEqual(filteredDict, {})
    
    def test_filter_unicode_singleWhitelist(self):
        """
        Test filter_dict to see if including an item on the whitelist keeps it
        Expected outcome is a dictionary with a single item (unicode)
        """
        whitelist = ['8dfa8f2b-4e13-48c1-8a49-d706ecfae5f2']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'bbox-north-lat': u'50.23343445'
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_unicode_multipleWhitelist(self):
        """
        Test filter_dict to see if including items on and off the whitelist 
            keeps them in the dict and excluding items removes them
        Expected outcome is a dictionary with multiple items (unicode)
        """
        whitelist = [
            'edcb7683-79a8-4b0d-8524-a8c0d313f932',
            'c124c58b-1723-4d7d-908e-772ef7713950',
            '8dfa8f2b-4e13-48c1-8a49-d706ecfae5f2'
        ]
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'bbox-north-lat': u'50.23343445',
            u'bbox-east-long': u'-54.9435608299',
            u'bbox-south-lat': u'52.33428394'
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_int_singleWhitelist(self):
        """
        Test filter_dict to see if including an item on the whitelist keeps it
        Expected outcome is a dictionary with a single item (int)
        """
        whitelist = ['e3a743d7-7108-4101-96b0-91750dbc151f']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'num_tags': 17
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_int_multipleWhitelist(self):
        """
        Test filter_dict to see if including an item on the whitelist keeps it
        Expected outcome is a dictionary with a multiple items (int)
        """
        whitelist = ['0964c7f8-f601-4b48-892b-6898b0bf42a5', 'e3a743d7-7108-4101-96b0-91750dbc151f']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'num_resources': 1,
            u'num_tags': 17
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_dict_singleWhitelist(self):
        """
        Test filter_dict to see if including an item within a dict on the whitelist keeps it
        Expected outcome is a dictionary with a single dictionary containing one item
        """
        whitelist = ['f028aef9-951c-44bb-906d-f3d50e8d1782']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'tracking_summary':{'total':6}
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_dict_mutlipleWhitelist(self):
        """
        Test filter_dict to see if including multple items on the whitelist keep them
        Expected outcome is a dictionary with a single dictionary containing multiple items
        """
        whitelist = ['f028aef9-951c-44bb-906d-f3d50e8d1782', '550dd97c-2462-4106-a126-12764ad243e3']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        compareDict = {
            u'tracking_summary':{'total':6, 'recent':6}
        }
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_encodedDict_singleWhitelist(self):
        """
        Test filter_dict to see if only including a single nested attribute will remove the others
        Expected outcome is the nested dict to be re-encoded, but without an attribute
        """
        whitelist = ['63ab7274-fe8c-457c-8162-58ee614dafd8']
        compareDict= {u'temporal-extent':u'{"begin": "2014-09-02"}'}
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_encodedDict_allWhitelist(self):
        """
        Test filter_dict to see if including all the items in a encoded dict on the whitelist
            will return the same dict
        """
        compareDict= {u'temporal-extent':u'{"begin": "2014-09-02", "end": "2018-01-02"}'}
        whitelist = ['e6f6cd2f-ae22-4a01-9c9c-73da48dc2e9a','63ab7274-fe8c-457c-8162-58ee614dafd8', '1b72740e-e314-4278-ba3c-9a38f8433fe8']
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        self.assertDictEqual(filteredDict, compareDict)

    #TODO Check if this is possible
    def test_filter_list_singleWhitelist(self):
        """
        Test filter_dict to see if excluding a component of a list item will remove it
            when returning the list item
        Expected outcome to return a dictionary with a list item (of a single dict)
        """
        pass

    def test_filter_list_allWhitelist(self):
        """
        Test filter_dict to see if including a list item on the whitelist will keep the entire list
        Expected outcome to return a dictionary with a list item (of multiple dicts)
        """
        whitelist = ['68661db8-de9b-4773-847d-1dcd73106cb1']
        compareDict = {u'resources': [{u'state':u'active', 'tracking_summary':{'total':0, 'recent':0}}]}
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_encodedList_stringified(self):
        """
        Test filter_dict to see if whitelisting a variable marked as stringified
            will return as a stringified list
        Expected outcome to return a list encoded as a string with no variables changed
        """
        whitelist = ['ee2c9a3b-865d-4aa8-a251-dca6a6b7427d']
        compareDict = {
            u'dataset-reference-date': u'[{"type": "creation", "value": "2014-09-02"}, {"type": "revision", "value": "2018-01-02"}]'
        }
        filteredDict = self.test_filterDict.filter_dict(self.testDict, self.testFields, whitelist)
        self.assertDictEqual(filteredDict, compareDict)

    def test_filter_wrongType(self):
        """
        Test filter_dict to see if it can handle receiving an unxpected object type
        Expected outcome is throwing a TypeError
        """
        with self.assertRaises(TypeError):
            self.test_filterDict("Wrong type", {}, {})
        with self.assertRaises(TypeError):
            self.test_filterDict(['W', 'R', 'O', 'N', 'G'], {}, {})
        with self.assertRaises(TypeError):
            self.test_filterDict(100, {}, {})
    
    def test_filter_emptyAll(self):
        """
        Test filter_dict to see if it can handle receiving an empty dictionary
        Expected outcome is an empty dictionary
        """
        filteredDict = self.test_filterDict.filter_dict({}, {}, {})
        self.assertIsInstance(filteredDict, dict)
        self.assertDictEqual(filteredDict, {})
        pass

# Tests the methods in the class that raise not implemented errors
class TestNotImplemented(unittest.TestCase):
    """
    Class for testing various methods that raise not implemented errors in
        meta_authorize
    Tests are simple as they are not expected to be called often / yet
    """

    test_notImplemented = meta_authorize.MetaAuthorize()

    def test_add_org(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.add_org("test")

    def test_get_orgs(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.get_orgs()
    
    def test_add_group(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.add_group("test")

    def test_get_groups(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.get_groups()

    def test_add_user(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.add_user("10000000")

    def test_get_users(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.get_users()

    def test_get_metadata_fields(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.get_metadata_fields("test")

    def test_get_visible_fields(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.get_visible_fields("testdataset", "testuser")

    def test_set_visible_fields(self):
        with self.assertRaises(NotImplementedError):
            self.test_notImplemented.set_visible_fields("test", "test", [])

# Required to run unit test
if __name__ == '__main__':
    unittest.main()