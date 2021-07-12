import logging
import uuid
import copy
import constants

from ckanext.vitality_prototype.meta_authorize import MetaAuthorize, MetaAuthorizeType

from pprint import pprint

import ckan
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.common import config


log = logging.getLogger(__name__)


class Vitality_PrototypePlugin(plugins.SingletonPlugin):
    """ 
    A CKAN plugin for creating a data registry.

    ...

    Attributes
    ----------
    meta_authorize : GraphMetaAuth
        Ckan authorization object.

    Methods
    -------
    (Include list of public methods here.)
        
    """

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IPackageController, inherit=True)


    # Authorization Interface
    meta_authorize = None

    # IConfigurer

    def update_config(self, config_):
        """Updates the CKAN configuration based on config_ via meta_authorize parameter.

        Parameters
        ----------
        config_ : config object
        
        Returns
        -------
        None
        """

        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'vitality_prototype')

        # Load neo4j connection parameters from config
        # Initalize meta_authorize
        self.meta_authorize = MetaAuthorize.create(MetaAuthorizeType.GRAPH, {
            'host': config.get('ckan.vitality.neo4j.host', "bolt://localhost:7687"),
            'user': config.get('ckan.vitality.neo4j.user', "neo4j"),
            'password': config.get('ckan.vitality.neo4j.password', "password")
        })
        
    # IPackageController -> When displaying a dataset
    def after_show(self,context, pkg_dict):
        
        log.info("Context")
        log.info(context)

        # Skip during indexing
        if context['user'].encode('utf-8') == 'default':
            return

        # If there is no authed user, user 'public' as the user id.
        user_id = None

        if context['auth_user_obj'] == None:
            user_id = 'public'   
        else:
            user = context['auth_user_obj']
            user_id = user.id


        # Decode unicode id...
        dataset_id = pkg_dict["id"].encode("utf-8")

        # Load dataset fields
        dataset_fields = self.meta_authorize.get_metadata_fields(dataset_id)
        
        # Load white-listed fields
        visible_fields = self.meta_authorize.get_visible_fields(dataset_id, user_id)

        log.info("Original")
        log.info(pkg_dict)

        # Filter metadata fields
        filtered = self.meta_authorize.filter_dict(pkg_dict, dataset_fields, visible_fields)

        # Replace pkg_dict with filtered
        pkg_dict.clear()
        for k,v in filtered.items():
            pkg_dict[k] = v

        log.info("after filtering:")
        log.info(pkg_dict)

        return pkg_dict

    def after_search(self, search_results, search_params):

        log.info('# of results ' + str(len(search_results)))


        for item in search_results['results']:
            for entry in item.keys():
                log.info(entry)

        return search_results

    def after_create(self, context, pkg_dict):
        log.info("HIT after_create")


    def after_update(self, context, pkg_dict):
        log.info("HIT after update")

    def before_index(self, pkg_dict):
        log.info("hit before_index")

        dataset_id = pkg_dict["id"].encode("utf-8")

        self.meta_authorize.add_dataset(dataset_id, generate_default_fields(), pkg_dict['owner_org'])

        log.info("type of metadata_fields: " + str(type(self.meta_authorize.get_metadata_fields(dataset_id))))

        # Set visible fields for all users in the authorization model.
        for user in self.meta_authorize.get_users():
            # Skip public user, we handle that as a special case after.
            if user == 'public':
                continue

            self.meta_authorize.set_visible_fields(
                dataset_id,
                user,
                generate_whitelist(
                    self.meta_authorize.get_metadata_fields(pkg_dict["id"])
                    )
                )

        # Set visible fields for the public/not-logged in users
        self.meta_authorize.set_visible_fields(
            dataset_id,
            'public',
            generate_whitelist(
                default_public_fields(self.meta_authorize.get_metadata_fields(pkg_dict["id"]))
            )
        )


        return pkg_dict

'''
Utility for printing pkg_dict structure
'''
def print_expanded(pkg_dict, key_name=None, depth=None):

    if depth == None:
        depth = 0
    
    last_key = None
    try:
        log.info(compute_tabs(depth) + ("Contents:" if key_name == None else (key_name + " Contents:") ))
        log.info(compute_tabs(depth) + "-------------")
        for key,value in pkg_dict.items():
            last_key = key
            if type(value) is dict:
                # If the value is another dictionary recurse!
                print_expanded(value, key_name=key, depth=depth+1)

            elif type(value) is list:

                for element in value:
                    print_expanded(element, key_name=key, depth=depth+1)

            else:
                log.info(compute_tabs(depth) + str(key) + ": ["+str(type(value))+"]"+ str(value))

    except Exception as ex:
        log.error("Error expanding pkg_dict. last_key:" + last_key)
        log.error(type(ex))
        log.error(str(ex))



def compute_tabs(num):
    """ Returns a string with `num` tabs
    """

    result = ""
    count = num
    while count > 0:
        result += "\t"
        count -= 1  
    
    return result

'''
UTILITY:
Returns a deep copy of a set of fields, which can then be used as a user's visibility entries 
'''
def generate_whitelist(fields):
    """ Returns a deep copy of a set of fields to use as user's visible entries.
    """

    return copy.deepcopy(fields)

def default_public_fields(fields):
    '''
    Returns a dictionary containing only the default public fields.
    '''

    # default_keys = ['id', 'notes_translated', 'notes', 'resources', 'type', 'name', 'state', 'organization]
    # result = {k: v for k, v in result.items() if k.encode('utf-8') not in default_keys}
    # fields.clear()
    # return result

    # Result dict
    result = copy.deepcopy(fields)

    for key in result.keys():
        key = key.encode('utf-8')
        if (key not in constants.PUBLIC_FIELDS):
            result.pop(key, None)

    return result


def generate_default_fields():
    """ 
    Generates a dictionary containing the default fields and associated uuids.

    Parameters
    ----------
    None

    Returns
    -------
    A dictionary containing hard-coded default values as keys and their corresponding uuids as values.
    """

    # TODO - Structure these field names for easier readability.
    # TODO - Consider benefit of including descriptors in the code.
    # TODO - Does this deserve its own class constant?
    field_names = constants.DATASET_FIELDS

    # Generate uuids for result dictionary.
    # return {k: str(uuid.uuid4()) for k in field_names}
    result = {}

    for entry in field_names:
        result[entry] = str(uuid.uuid4())

    return result