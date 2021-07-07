from enum import Enum
import logging
import json
import copy
from flatten_dict import flatten
from flatten_dict import unflatten


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

    def filter_dict(self, input, fields, whitelist):
        """
        Filter a dictionary, returning only white-listed keys.

        Parameters
        ----------
        input : dict
            The dictionary to filter.
        fields: dict
            Dictionary representing the fields and ids the dictionary should contain.
        whitelist: list of uuids
            The list of permitted field ids.

        Returns
        -------
        the original dictionary input with fields corresponding to whitelist.      
        """

        # Trivially check input type
        if type(input) != dict:
            raise TypeError("Only dicts can be filtered recursively! Attempted to filter " + str(type(input)))

        # DECODE Stringified JSON elements
        decoded = self._decode(copy.deepcopy(input))

        # FLATTEN decoded input
        flattened = flatten(decoded, reducer='path')

        # Iterate through the dictionary entries

        # TODO: I would use a comprehension + helper function.
        #  i.e. {k: v for k, v in input.items if filterLogicFn(k, v)}
        #  The original dictionary will stay in tact and in memory though.
        for key,value in flattened.items():

            log.info("Checking authorization for %s", str(key))

            # Pop unknown fields
            if key.encode('utf-8') not in fields:
                flattened.pop(key, None)
                log.warn("Popped unknown field: " + str(key))
                continue

            # Get field id corresponding with key
            curr_field_id = fields[key.encode('utf-8')]

            # If the current field's id does not appear in the whitelist, pop it from the input
            if curr_field_id not in whitelist:
                flattened.pop(key, None)
                log.info("Key rejected!")
                continue

            # If the value is a dict, recurse 
            if type(value) is dict:
                
                # Overwrite value with filtered dict
                flattened[key] = self.filter_dict(value, fields, whitelist)
            
            log.info("Key authorized!")

        # UNFLATTEN filtered dictionary
        unflattened = unflatten(flattened, splitter='path')

        # STRIGIFY required json fields
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

            if key in _strigified_keys():
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


def _strigified_keys():
    """
    Returns a list of keys whose values should be strigified json objects
    """
    return [
        "metadata-point-of-contact",
        "spatial",
        "temporal-extent",
        "unique-resource-identifier-full",
        "notes",
        "cited-responsible-party",
        "dataset-reference-date"
    ]