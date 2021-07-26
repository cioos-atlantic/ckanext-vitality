from enum import Enum
import logging
import json
import copy
import constants
from flatten_dict import flatten
from flatten_dict import unflatten
import uuid


'''
Enumeration of MetaAuthorize implementations 
'''
class MetaAuthorizeType(Enum):
    SIMPLE = 0 # JSON file based
    GRAPH = 1 # Neo4J based

log = logging.getLogger(__name__)



class MetaAuthorize(object):
    """ 
    An authorization tool for displaying metadata in CKAN

    ...

    Methods
    ------- 
    add_org(org_id)
        Adds a organization org_id to the authorization model.

    get_orgs()
        Returns a list of organization ids.

    add_group(group_id)
        Adds a group to the authorization model.

    get_groups()
        Returns a list of group ids.

    filter_dict(input, fields, whitelist)
        Filters a dictionary input to conform to the model fields if the key is in whitelist.

    Notes
    -----
    For information on the Organization, Groups and other traits, 
    see the `CKAN documentation <https://docs.ckan.org>`

    """

    @staticmethod
    def create(type, opts):
        # Do imports in create to avoid circular imports
        from ckanext.vitality_prototype.impl.simple_meta_auth import _SimpleMetaAuth
        from ckanext.vitality_prototype.impl.graph_meta_auth import  _GraphMetaAuth

        result = None
        if type is MetaAuthorizeType.SIMPLE:
            result = _SimpleMetaAuth()
            result.__load()
        elif type is MetaAuthorizeType.GRAPH:
            result = _GraphMetaAuth(opts['host'], opts['user'],  opts['password'])
        else:
            log.error("Unknown MetaAuthorize Implementation type!")

        return result

    def add_org(self, org_id):
        """ 
        Add an organization to the authorization model with org_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_org(self, org_id)" % (self.__class__.__name__))

    def get_orgs(self):
        """
        Returns a list of org_ids.
        """
        raise NotImplementedError("Class %s doesn't implement get_orgs(self)" % (self.__class__.__name__))

    def add_group(self, group_id):
        """
        Add a group to the authorization model with group_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_group(self, group_id)" % (self.__class__.__name__))

    def get_groups(self):
        """
        Returns a list of group ids from authorization model.
        """
        raise NotImplementedError("Class %s doesn't implement get_groups(self)" % (self.__class__.name))

    def add_user(self, user_id):
        """
        Add a user to the authorization model with the user_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_user(self, user_id)" % (self.__class__.__name__))

    def get_users(self):
        """
        Get a list of user_id s based on the current authorization model.
        """

        raise NotImplementedError("Class %s doesn't implement get_users(self)" % (self.__class__.__name__))

    def add_dataset(self, dataset_id, fields, owner_id):
        """
        Add a dataset with the current id (dataset_id), fields and owner_id to the authorization model.
        """

        raise NotImplementedError("Class %s doesn't implement add_dataset(self, dataset_id, fields, owner_id)" % (self.__class__.__name__))

    def add_metadata_fields(self, dataset_id, field):
        """
        Add a field to the current dataset in the authorization model.
        """

        raise NotImplementedError("Class %s doesn't implement add_metadata_fields(self, dataset_id, field)" % (self.__class__.__name__))


    def get_metadata_fields(self, dataset_id):
        """
        Return a dictionary of metadata fields based on the dataset_id
        """

        raise NotImplementedError("Class %s doesn't implement get_metadata_fields(self, dataset_id)" % (self.__class__.__name__))


    def get_visible_fields(self, dataset_id, user_id):
        """
        Return the subset of metadata field ids in this dataset for which the user has access.
        """
        
        raise NotImplementedError("Class %s doesn't implement get_visible_fields(self, dataset_id, user_id)")


    def set_visible_fields(self, dataset_id, user_id, whitelist):
        """
        Set the visible fields for the particular user_id.
        """

        raise NotImplementedError("Class %s doesn't implement set_visible_fields(self, dataset_id, user_id, whitelist)" % (self.__class__.__name__))
    
    def keys_match(self, unfiltered_content, known_fields):
        """
        """
        flattened = {(k, uuid.uuid4()) for k in flatten(self._decode(unfiltered_content), reducer='path').keys() if k not in known_fields.keys()}
        
        #TODO Throw error if important fields are removed
        return flattened

    def filter_dict(self, unfiltered_content, fields, whitelist):
        """
        Filter a dictionary, returning only white-listed keys.

        Parameters
        ----------
        unfiltered_content : dict
            The dictionary to filter.
        fields: dict
            Dictionary representing the fields and ids the dictionary should contain.
        whitelist: list of uuids
            The list of permitted field ids.

        Returns
        -------
        a new dictionary with keys and values corresponding to fields and whitelist.      
        """

        def is_public(key):
            """
            Predicate to establish that a key entry can be made public based on data in fields.

            Parameters
            ----------
            key: unicode
                The encoded string key for the entry.

            Returns
            -------
            True if the entry should be seen by public and False if not.
            """
            key_string = key.encode("UTF-8")

            return key_string in fields and fields[key_string] in whitelist

        def test_if_flat(key, val):
            """
            Helper function to handle recursion on the case of val being a dict, otherwise 
            returns the value if whitelisted or None.

            Parameters
            ----------
            key: string
                The parameter name
            val: string or dict
                The parameter value
            
            Returns
            -------
            The recursed value of for key or None if 
            """
            if not isinstance(val, dict):
                return val
            else:
                return self.filter_dict(val, fields, whitelist)

        # Trivially check input type
        if not isinstance(unfiltered_content, dict):
            raise TypeError("Only dicts can be filtered recursively! Attempted to filter " + str(type(input)))
        flattened = {k: test_if_flat(k, v) for k, v in flatten(self._decode(unfiltered_content), reducer='path').items() if is_public(k)}
        # do not clear the original dictionary, which is needed for admin access.
        # UNFLATTEN filtered dictionary
        log.info("Flattened dict {}".format(flattened))
        unflattened = unflatten(flattened, splitter='path')
        # STRINGIFY required json fields
        encoded = self._encode(unflattened)  
        return encoded

    
    def _decode(self, input):
        """
        Decode dictionary containing string encoded JSON objects. 

        Parameters
        ----------
        input: dict or stringified JSON
            The dictionary to decode

        Returns
        -------
        A dictionary where all fields that contained stringified JSON are now 
        expanded into dictionaries. 
        """
        if type(input) == str or type(input) == unicode:
            root = MetaAuthorize._parse_json(input)
        elif type(input) == dict:
            root = input
        else:
            raise TypeError("_decode can only decode str or dict inputs! Got {}".format(str(type(input))))

        if root != None:
            for key,value in root.items():
                # If the value is a string attempt to parse it as json
                #log.info("Attempting to decode: %s - %s ", key, str(type(value)))
                #TODO - this may need to change for python3
                if type(value) == str or type(value) == unicode:
                    #log.info("%s is a str/unicode!", key)
                    parsed_json = MetaAuthorize._parse_json(value, key)

                    # If the string parsed 
                    if parsed_json != None:
                        # into a dictonary 
                        if type(parsed_json) == dict:
                            # decode the parsed dict
                            parsed_json = self._decode(parsed_json)
                            log.info('%s - parsed type %s', key, type(parsed_json))
                            # replace the value at the current key
                            root[key] = parsed_json
                        # into a list
                        elif type(parsed_json) == list:
                            # replace the value at the current key
                            root[key] = parsed_json


                # Else if the value is a dictonary, recurse!
                elif type(value) == dict:
                    root[key] = self._decode(value)

                
        return root

    def _encode(self, input):

        for key,value in input.items():

            if key in constants.STRINGIFIED_FIELDS:
                log.info("Stringifying %s", key)
                input[key] = unicode(json.dumps(value),'utf-8')

        return input


    @staticmethod
    def _parse_json(value, key=None):
        try:
            # TODO: Unicode stuff may need rework for python 3
            return json.loads(value.encode('utf-8'))
        except ValueError:
            #log.info("Value could not be parsed as JSON. %s", key)
            return None
        except TypeError:
            #log.warn("Value could not be parsed as JSON, %s", key)
            return None
