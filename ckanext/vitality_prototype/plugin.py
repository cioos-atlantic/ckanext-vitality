import logging
import uuid
import copy

from ckanext.vitality_prototype.meta_authorize import MetaAuthorize, MetaAuthorizeType


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

        self.meta_authorize.filter_dict(pkg_dict, dataset_fields, visible_fields)


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
                #If the value is another dictionary recurse!
                print_expanded(value, key_name=key, depth=depth+1)

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

    # Result dict
    result = copy.deepcopy(fields)

    for key in result.keys():
        if (key.encode('utf-8') != "id" and 
            key.encode('utf-8') != "notes_translated" and 
            key.encode('utf-8') != "notes" and
            key.encode('utf-8') != "resources" and
            key.encode('utf-8') != "type" and 
            key.encode('utf-8') != "name" and
            key.encode('utf-8') != "state" and
            key.encode('utf-8') != "organization"
            ):
            result.pop(key, None)

    return result


def generate_default_fields():
    """ Hard-coded fields for each dataset.
    """

    field_names = [
        "notes_translated",
        "bbox-east-long",
        "license_title",
        "maintainer",
        "author",
        "relationships_as_object",
        "citation",
        "resource-type",
        "bbox-north-lat",
        "private",
        "maintainer_email",
        "num_tags",
        "xml_location_url",
        "keywords",
        "metadata-language",
        "id",
        "metadata_created",
        "title_translated",
        "cited-responsible-party",
        "metadata_modified",
        "bbox-south-lat",
        "author_email",
        "metadata-point-of-contact",
        "state",
        "spatial",
        "progress",
        "type",
        "resources",
        "creator_user_id",
        "num_resources",
        "tags",
        "bbox-west-long",
        "dataset-reference-date",
        "tracking_summary",
        "total",
        "recent",
        "metadata-reference-date",
        "frequency-of-update",
        "groups",
        "license_id",
        "relationships_as_subject",
        "temporal_extent",
        "organization",
        "approval_status",
        "created",
        "title",
        "description_translated",
        "image_url_translated",
        "name",
        "is_organization",
        "state",
        "image_url",
        "revision_id",
        "title_translated",
        "description",
        "unique-resource-identifier-full",
        "isopen",
        "url",
        "notes",
        "owner_org",
        "extras",
        "license_url",
        "eov",
        "revision_id",
        "vertical-extent",
        "temporal-extent"
    ]

    # Result dict
    result = {}

    for entry in field_names:
        result[entry] = str(uuid.uuid4())

    return result