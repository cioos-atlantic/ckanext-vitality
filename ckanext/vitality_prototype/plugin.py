import logging
import uuid
import copy
import constants
import json

from ckanext.vitality_prototype.meta_authorize import MetaAuthorize, MetaAuthorizeType

from pprint import pprint

import ckan
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.plugins.interfaces as interfaces
from ckan.common import config


log = logging.getLogger(__name__)

 



@toolkit.chained_action
def organization_delete(action, context, data_dict=None):
    log.info("An organization has been deleted")
    return action(context, data_dict)

@toolkit.chained_action
def user_delete(action, context, data_dict=None):
    log.info("An user has been deleted")
    return action(context, data_dict)

@toolkit.chained_action
def package_create(action, context, data_dict=None):
    log.info("An package has been created")
    return action(context, data_dict)

@toolkit.chained_action
def package_update(action, context, data_dict=None):
    log.info("An package has been updated")
    # Only needs to track description?
    return action(context, data_dict)

@toolkit.chained_action
def package_delete(action, context, data_dict=None):
    log.info("An package has been deleted")
    # Only needs to track description?
    return action(context, data_dict)

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
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions, inherit=True)

    # Authorization Interface
    meta_authorize = None

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'vitality_prototype_get_minimum_fields': lambda: constants.MINIMUM_FIELDS
        }

    
    def get_actions(self):
        log.info("got actions")
        return {
            "organization_update" : self.organization_update,
            "organization_create" : self.organization_create,
            "organization_delete" : organization_delete,
            "organization_member_create" : self.organization_member_create,
            "organization_member_delete" : self.organization_member_delete,
            "user_update" : self.user_update,
            "user_create" : self.user_create,
            "user_delete" : user_delete,
            "package_update" : package_update,
            "package_create" : package_create,
            "package_delete" : package_delete
        }

    @toolkit.chained_action
    def organization_member_create(self, action, context, data_dict=None):
        log.info("A member has been added!")
        org_id= data_dict['id']
        user_id = self.meta_authorize.get_user_by_username(data_dict['username'])['id']
        # Get roles for org
        org_roles = self.meta_authorize.get_roles(org_id)
        log.info(org_roles)
        log.info(org_roles['member'])
        self.meta_authorize.set_user_role(user_id, org_roles['member'])
        # Add user to member role for organization
        return action(context,data_dict)

    @toolkit.chained_action
    def organization_member_delete(self, action, context, data_dict=None):
        log.info("A member has been deleted")
        org_id= data_dict['id']
        user_id = data_dict['user_id']
        log.info("Collected ids")
        log.info(org_id)
        log.info(user_id)
        role_id = self.meta_authorize.get_roles(org_id)['member']
        self.meta_authorize.detach_user_role(user_id, role_id)
        return action(context,data_dict)

    # Triggers when an org's information is updated (name)
    @toolkit.chained_action
    def organization_update(self, action, context, data_dict=None):
        log.info("An organization has been edited")
        ckan_org_info = toolkit.get_action('organization_show')(context, data_dict)
        ckan_org_id= ckan_org_info['id']
        ckan_org_name = data_dict['name']
        neo4j_org_name = self.meta_authorize.get_organization(ckan_org_id)['name']
        if(neo4j_org_name != ckan_org_name):
            log.info("Org name has been updated")
            self.meta_authorize.set_organization_name(ckan_org_id, ckan_org_name)
            org_name = self.meta_authorize.get_organization(ckan_org_id)['name']
        return action(context, data_dict)      

    # Triggers when a user's information is updated (name/email)
    @toolkit.chained_action
    def user_update(self, action, context, data_dict=None):
        log.info("An user has been edited")
        ckan_user_info = toolkit.get_action('user_show')(context,data_dict)
        neo4j_user_info = self.meta_authorize.get_user(ckan_user_info['id'])
        # Unsure if username can be changed, but this can work around it if so
        if(data_dict['name'] != neo4j_user_info['username']):
            log.info("Username has been updated")
            self.meta_authorize.set_user_username(ckan_user_info['id'], data_dict['name'])
        if(data_dict['email'] != neo4j_user_info['email']):
            log.info("Email has been updated")
            self.meta_authorize.set_user_email(ckan_user_info['id'], data_dict['email'])
        return action(context, data_dict)

    @toolkit.chained_action
    def user_create(self, action, context, data_dict=None):
        result = action(context, data_dict)
        log.info("A user has been created")
        log.info(result)
        user_id = result['id']
        user_name = result['name']
        user_email = result['email']
        self.meta_authorize.add_user(user_id, user_name, user_email)
        if(result['sysadmin']):
            log.info('add to admin role')
            self.meta_authorize.set_user_role(user_id, 'admin')
        # Create user in neo4j
        return result

    @toolkit.chained_action
    def organization_create(action, context, data_dict=None):
        log.info("An organization has been created")
        return action(context, data_dict)

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

        log.info("Now checking if this is beforeIndex")
        log.info(context['package'].type)
        if context['package'].type != 'dataset':
            log.info("This pkg is not a dataset. Let it through unchanged.")
            return pkg_dict

        # Skip during indexing
        if (type(context['user']) == str or type(context['user']) == unicode) and context['user'].encode('utf-8') == 'default':
            log.info("This is before index we're done here.")
            return pkg_dict


        log.info("This is not before index, filtering")
        log.info("Initial pkg_dict:")
        log.info(pkg_dict)

        # Description
        notes = pkg_dict['notes'].encode("UTF-8")

        # Decode unicode id...
        dataset_id = pkg_dict["id"].encode("utf-8")

        # Check to see if the dataset has just been created
        if(self.meta_authorize.get_dataset(dataset_id) == None):
            log.info("Dataset not in model yet. Returning")
            return pkg_dict

        # If there is no authed user, user 'public' as the user id.
        user_id = None
        if context['auth_user_obj'] == None:
            user_id = 'public'   
        else:
            user = context['auth_user_obj']
            user_id = user.id

        # Load dataset fields
        dataset_fields = self.meta_authorize.get_metadata_fields(dataset_id)
        
        # Load white-listed fields
        visible_fields = self.meta_authorize.get_visible_fields(dataset_id, user_id)


        #TODO check for keys matching
        extra_keys = self.meta_authorize.keys_match(pkg_dict, dataset_fields)
        if extra_keys != set():
            log.info("Extra keys found!")
            log.info(extra_keys)
            self.meta_authorize.add_metadata_fields(dataset_id, extra_keys)
            #TODO Call and implement add metadata fields

        # Filter metadata fields
        filtered = self.meta_authorize.filter_dict(pkg_dict, dataset_fields, visible_fields)

        # Replace pkg_dict with filtered
        pkg_dict.clear()
        for k,v in filtered.items():
            pkg_dict[k] = v


        # Inject public visibility settings
        pkg_dict['public-visibility'] = self.meta_authorize.get_public_fields(dataset_id)

        # Inject empty resources list if resources has been filtered.
        if 'resources' not in pkg_dict:
            pkg_dict['resources'] = []

        # Inject empty xml link if it has been filtered
        if 'xml_location_url' not in pkg_dict:
                pkg_dict['xml_location_url'] = ""

        log.info("Final pkg_dict:")
        log.info(pkg_dict)
        return pkg_dict

    def after_search(self, search_results, search_params):

        # Gets the current user's ID (or if the user object does not exist, sets user as 'public')
        if toolkit.c.userobj == None:
            log.info('Public user')
            user_id = 'public'   
        else:
            user = toolkit.c.userobj
            user_id = user.id
            log.info(user)
            log.info('Request from ' + user_id)

        # Gets the number of results matching the search parameters (total)
        log.info('# of total results ' + str(search_results['count']))

        # However, at a time only loads a portion of the results
        datasets = search_results['results']
        result_count = len(datasets)
        log.info('Number of results loaded' + str(result_count))        
        
        # Gets the total number of results matching the search parameters
        log.info('Number of results ' + str(len(search_results)))

        # Go through each of the datasets returned in the results
        log.info('Parsing search data')
        for x in range(len(datasets)):
            pkg_dict = search_results['results'][x]
            # Loop code is copied from after_show due to pkg_dict similarity
            # Decode unicode id...
            dataset_id = pkg_dict["id"].encode("utf-8")

            # Load dataset fields
            dataset_fields = self.meta_authorize.get_metadata_fields(dataset_id)
            
            # Load white-listed fields
            visible_fields = self.meta_authorize.get_visible_fields(dataset_id, user_id)

            # If no relation exists between user and dataset, treat as public
            if len(visible_fields) == 0:
                visible_fields = self.meta_authorize.get_visible_fields(dataset_id, 'public')
                log.info('Public visible fields  ' + str(visible_fields))

            # Filter metadata fields
            filtered = self.meta_authorize.filter_dict(pkg_dict, dataset_fields, visible_fields)

            # Replace pkg_dict with filtered
            pkg_dict.clear()
            for k,v in filtered.items():
                pkg_dict[k] = v

            # Inject public visibility settings
            pkg_dict['public-visibility'] = self.meta_authorize.get_public_fields(dataset_id)

            # Inject empty resources list if resources has been filtered.
            if 'resources' not in pkg_dict:
                pkg_dict['resources'] = []

            # If the metadata is restricted in any way will add a "resource" so a tag can be generated
            # TODO Check if restricted for current user AS WELL AS for public user (so we can harvest in as restricted)
            pkg_dict['resources'].append({"format" : "Restricted data"})

            # If current user does not have full access to the metadata, tag the dataset as such
            user_dataset_access = self.meta_authorize.get_template_access_for_user(dataset_id, user_id)
            if(user_dataset_access != "Full"):
                pkg_dict['resources'].append({"format" : "Restricted metadata"})

            # Add filler for specific fields with no value present so they can be harvested
            if 'notes_translated' not in pkg_dict or not pkg_dict['notes_translated']:
                pkg_dict['notes_translated'] = {"fr": "-", "en":"-"}
            if 'metadata-point-of-contact' not in pkg_dict or not pkg_dict['metadata-point-of-contact']:
                pkg_dict['metadata-point-of-contact'] = "{\"contact-info_online-resource\": \"-\", \"position-name\": \"-\", \"contact-info_email\": \"-\", \"role\": \"-\", \"organisation-name\": \"-\", \"individual-name\": \"-\"}"
            if 'cited-responsible-party' not in pkg_dict or not pkg_dict['cited-responsible-party']:
                pkg_dict['cited-responsible-party'] = "[{\"contact-info_online-resource\": \"-\", \"position-name\": \"-\", \"contact-info_email\": \"-\", \"role\": \"-\", \"organisation-name\": \"-\", \"individual-name\": \"-\"}]"
            
            # Though xml_location_url is also required for harvest, it is not included with the public variables
            #   as it displays the scrubbed metadata in the xml
            if 'xml_location_url' not in pkg_dict or not pkg_dict['xml_location_url']:
                pkg_dict['xml_location_url'] = '-'
            
        return search_results

    def after_create(self, context, pkg_dict):
        log.info("HIT after_create")
        return pkg_dict

    def after_update(self, context, pkg_dict):
        log.info("HIT after update")
        log.info(context)

        log.info("Updated pkg_dict")
        log.info(pkg_dict)

        # Only update public visibility settings if the field exists in pkg_dict
        if 'public-visibility' not in pkg_dict:
            return pkg_dict

        # Decode unicode id...
        dataset_id = pkg_dict["id"].encode("utf-8")

        # extract/load public visibility settings
        publically_visible_fields = json.loads(pkg_dict['public-visibility'])

        # ensure minimum fields are always visible
        for min_field in constants.MINIMUM_FIELDS:
            if min_field not in publically_visible_fields:
                publically_visible_fields.append(min_field)

        # create a new public visibility whitelist
        whitelist = {}
        for f in self.meta_authorize.get_metadata_fields(dataset_id).items():
            if f[0] in publically_visible_fields:
                whitelist[f[0]] = f[1]
        
        self.meta_authorize.set_visible_fields('public', whitelist)

        return pkg_dict

    def before_index(self, pkg_dict):
    
        log.info("hit before_index")

        dataset_id = pkg_dict["id"].encode("utf-8")

        # Generate the default templates (full and min). For non-default templates use uuid to generate ID

        self.meta_authorize.add_dataset(dataset_id, pkg_dict['owner_org'], dname=pkg_dict['title'])

        log.info("adding default templates")
        templates = self.meta_authorize.get_templates(dataset_id)
        if len(templates) == 0:
            if 'notes' in pkg_dict and pkg_dict['notes']:
                log.info(type(pkg_dict['notes']))
                try:
                    dataset_notes = json.loads(pkg_dict['notes'].encode('utf-8'))
                    self.meta_authorize.set_dataset_description(dataset_id, "en", dataset_notes['en'])
                    self.meta_authorize.set_dataset_description(dataset_id, "fr", dataset_notes['fr'])
                except ValueError as err:
                    log.info("No description found")

            # Generate an id, name, and description for the default templates (full and minimal)
            # TODO Create a better description based on the final
            full_id = str(uuid.uuid4())
            full_name = 'Full'
            full_description = "This is the full, unrestricted template. Choosing this will display the full set of metadata for the assigned role."
            minimal_id = str(uuid.uuid4())
            minimal_name = "Minimal"
            minimal_description = "This template restricts some metadata for the chosen role. Restricted fields include location and temporal data"

            # Adds the full and minimal template
            self.meta_authorize.add_template_full(dataset_id, full_id, full_name, generate_default_fields(), full_description)
            self.meta_authorize.add_template(dataset_id, minimal_id, minimal_name, minimal_description)
            self.meta_authorize.set_visible_fields(
                minimal_id,
                generate_whitelist(
                    default_public_fields(self.meta_authorize.get_metadata_fields(dataset_id))
                )
            )
            
            log.info("type of metadata_fields: " + str(type(self.meta_authorize.get_metadata_fields(dataset_id))))

            # Always add access for public and admin roles
            # TODO Discuss change default for public?
            self.meta_authorize.set_template_access('public', minimal_id)
            self.meta_authorize.set_template_access('admin', full_id)

            # Add access for any roles in the organization
            log.info(self.meta_authorize.get_roles(pkg_dict['owner_org']))
            for role in self.meta_authorize.get_roles(pkg_dict['owner_org']).values():
                self.meta_authorize.set_template_access(str(role), full_id)

            #Add serves for organizations
            
            
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