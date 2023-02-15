import logging
from operator import truediv
from os import stat
from re import template
from ckanext.vitality.meta_authorize import MetaAuthorize
from neo4j import GraphDatabase
import uuid 
from ckanext.vitality import constants

log = logging.getLogger(__name__)

class _GraphMetaAuth(MetaAuthorize):
    """ Graph database authorization settings.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def __close(self):
        self.driver.close()

    def add_dataset(self, dataset_id, owner_id, dname=None):
        """
        Adds a dataset to the database and assigns an organization owner
        
        Parameters
        ----------
        dataset_id : string
            The UUID of the dataset to create
        owner_id : string
            The UUID of the organization that owns the dataset
        """
        with self.driver.session() as session:
            # Check to see if the dataset already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_dataset, dataset_id) != None:
                return
            session.write_transaction(self.__write_dataset, dataset_id, dname)
            session.write_transaction(self.__bind_dataset_to_org, owner_id, dataset_id)

    def add_group(self, group_id, users):
        """
        Adds a group to the database and binds users to membership
        
        Parameters
        ----------
        group_id : string
            The UUID of the group to create
        users : dict
            A dictionary of users to put into the group
        """
        with self.driver.session() as session:
            # Check to see if the group already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_group, group_id):
                return
            session.write_transaction(self.__write_group, group_id)
            for user in users:
                session.write_transaction(self.__bind_user_to_group, group_id, user['id'])

    def add_metadata_fields(self, dataset_id, fields, template_id):
        """
        Adds new metadata fields as elements to the dataset and attaches them to a template
        
        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to add the fields to
        fields : set
            A set of tuples with the name of the new field and a generated uuid
        template_id : string
            The id/uuid of the full template for the dataset
        """
        with self.driver.session() as session:
            existing_fields = session.read_transaction(self.__read_elements, dataset_id)
            existing_names = [x[0] for x in existing_fields.items()]
            # For every new field to add
            for f in fields:
                # Only add the new field if a field with that name doesn't already exist
                if f[0] not in existing_names:
                    session.write_transaction(self.__write_metadata_field, f[0], str(f[1]), template_id)

    def add_org(self, org_id, users, org_name=None):        
        """
        Adds new organization into the database and adds users to membership
        
        Parameters
        ----------
        org_id : string
            The id/uuid of the new organization
        users : list
            A list of user dictionaries containing user ids
        org_name : string
            The name of the new organization
        """
        with self.driver.session() as session:
            # Check to see if the org already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_org_by_id, org_id):
                return
            session.write_transaction(self.__write_org, org_id, org_name)
            # Create member role
            member_id = str(uuid.uuid4())
            session.write_transaction(self.__write_role, member_id, "member")
            session.write_transaction(self.__bind_role_to_org, member_id, org_id)
            for user in users:
                if not session.read_transaction(self.__has_role, user['id'], 'admin'):
                    session.write_transaction(self.__bind_user_to_role, user['id'], member_id)

    def add_role(self, id, name=None):        
        """
        Adds new role into the database
        
        Parameters
        ----------
        role_id : string
            The id/uuid of the new role
        role_name : string
            The name of the new role
        """
        with self.driver.session() as session:
            session.write_transaction(self.__write_role, id, name)

    def add_user(self, user_id, user_name = None, user_email = None, gid = None):
        """
        Adds new user into the database. If a user with that id already exists they will not be added
        
        Parameters
        ----------
        user_id : string
            The id/uuid of the new user
        user_name : string
            The username of the new user
        user_email : string
            The email of the new user
        gid : string
            The Google id of the new user
        """
        with self.driver.session() as session:
            # Check to see if the user already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_user_by_id, user_id) != None:
                return
            session.write_transaction(self.__write_user, user_id, user_name, user_email, gid)

    def add_template(self, dataset_id, template_id, template_name=None, template_description=None):
        """
        Adds new tenplate into the database and binds to a dataset
        
        Parameters
        ----------
        dataset_id : string
            The id/uuid of dataset to bind the template to
        template_id : string
            The id/uuid of new template
        template_name : string
            The name of the new template
        template_description : string
            The description of the new template
        """
        with self.driver.session() as session:
            session.write_transaction(self.__write_template, template_id, template_name, template_description)
            session.write_transaction(self.__bind_template_to_dataset, template_id, dataset_id)        
    
    # TODO Generate fields separately and set instead of two different instantiation methods
    def add_template_full(self, dataset_id, template_id, template_name, fields, template_description = None):
        """
        Adds new full tenplate into the database, binds to a dataset, and creates a set of fields to attach
        
        Parameters
        ----------
        dataset_id : string
            The id/uuid of dataset to bind the template to
        template_id : string
            The id/uuid of new template
        template_name : string
            The name of the new template
        fields : dictionary
            A dictionary field names as keys and their corresponding uuids as values.
        template_description : string
            The description of the new template
        """
        with self.driver.session() as session:
            # Default templates already exist, skipping
            if session.read_transaction(self.__read_templates, dataset_id):
                log.info("templates exist already")
                return
            # Add full template
            self.add_template(dataset_id, template_id, template_name, template_description)
            # Fullcreate the fields as well
            for name,id in fields.items():
                session.write_transaction(self.__write_metadata_field, name, id, template_id)

    def delete_dataset(self, dataset_id):
        """
        Deletes an dataset given its ID
        Also removes associated templates and elements
        
        Parameters
        ----------
        dataset_id : string
            The id/uuid of dataset to delete
        """
        with self.driver.session() as session:
            session.write_transaction(self.__delete_dataset, dataset_id)

    def delete_organization(self, org_id):
        """
        Deletes an organization given its ID
        
        Parameters
        ----------
        org_id : string
            The id/uuid of organization to delete
        """
        with self.driver.session() as session:
            session.write_transaction(self.__delete_organization, org_id)
            
    def delete_user(self, user_id):
        """
        Deletes an user given its ID
        
        Parameters
        ----------
        user_id : string
            The id/uuid of organization to delete
        """
        with self.driver.session() as session:
            session.write_transaction(self.__delete_user, user_id)

    def detach_user_role(self, user_id, role_id):
        """
        Deletes the relationship between a role and a give user
        
        Parameters
        ----------
        user_id : string
            The id/uuid of the user to detach the template from
        template_id : string
            The id/uuid of template to detach from the user
        """
        with self.driver.session() as session:
            session.write_transaction(self.__detach_user_from_role, user_id, role_id)

    def get_admins(self):
        """ 
        Gets a list of admin users in the database

        Parameters
        ----------
        None

        Returns
        -------
        A dictionary of users with 'id' as the key and the associated user id as the value
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_users_admins)

    def get_dataset(self, dataset_id):
        """ 
        When given a dataset id, returns the id if it exists in the database and None if it does not

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check

        Returns
        -------
        The dataset id if it exists and None if it does not
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__get_dataset, dataset_id)

    def get_metadata_fields(self, dataset_id):
        """ 
        Returns the elements managed by a dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check

        Returns
        -------
        The dataset id if it exists and None if it does not
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_elements, dataset_id)

    def get_organization(self, organization_id):
        """ 
        Returns the information of a given organization

        Parameters
        ----------
        organization_id : string
            The id/uuid of a organization to check

        Returns
        -------
        An organization object (name and id) if one exists, and none if one does not
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__get_org_by_id, organization_id)

    def get_public_fields(self, dataset_id):
        """ 
        Returns a list of fields in the dataset that are visible to the public / unconnected users

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset get visible fields for

        Returns
        -------
        An organization object (name and id) if one exists, and none if one does not
        """
        public_field_ids =  self.get_visible_fields(dataset_id, user_id='public')
        public_field_names = [f[0].encode("utf-8") for f in self.get_metadata_fields(dataset_id).items() if f[1] in public_field_ids]
        return public_field_names

    def get_roles(self, org_id = None):
        """ 
        Returns a list of roles in the database. If an organization is provided lists roles owned by that org

        Parameters
        ----------
        org_id : string (optional)
            The id/uuid of an organization to get roles from

        Returns
        -------
        A list of all roles (if no org provided) or roles owned by an organization (if org provided)
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_roles, org_id)


    def get_private_dataset(self, dataset_id):
        """ 
        Returns the UUID of the private version of a related dataset, if one exists

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check for a related dataset

        Returns
        -------
        The id of the related private dataset if one exists, if not then returns None
        """
        with self.driver.session() as session:
            related_dataset_id = session.read_transaction(self.__get_private_dataset, dataset_id)
            return related_dataset_id

    def get_public_dataset(self, dataset_id):
        """ 
        Returns the UUID of the public version of a related dataset, if one exists

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check for a related dataset

        Returns
        -------
        The id of the related public dataset if one exists, if not then returns None
        """
        with self.driver.session() as session:
            related_dataset_id = session.read_transaction(self.__get_public_dataset, dataset_id)
            return related_dataset_id

    def get_templates(self, dataset_id):
        """ 
        Returns a dictionary of available templates for a given dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to get templates from

        Returns
        -------
        A dictionary of the templates with the name as the key and the id as the value
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_templates, dataset_id)    
            
    def get_template_access_for_role(self, dataset_id, role_id):
        """ 
        Returns the template name assigned to a role for a given dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check template access for
        role_id : string
            The id/uuid of a role to check the assigned template for

        Returns
        -------
        The name of the template as a String
        """
        with self.driver.session() as session:
            template_id = session.read_transaction(self.__get_template_access_for_role, dataset_id, role_id)
            if template_id != None:
                template_name = session.read_transaction(self.__get_template_name, template_id)
                return str(template_name)
            else:
                return None

    def get_template_access_for_user(self, dataset_id, user_id):
        """ 
        Returns the template name assigned to a user (through a role) for a given dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to check template access for
        user_id : string
            The id/uuid of a user to check the assigned template for

        Returns
        -------
        The name of the template as a String
        """
        with self.driver.session() as session:
            template_id = session.read_transaction(self.__get_template_access_for_user, dataset_id, user_id)
            if template_id != None:
                template_name = session.read_transaction(self.__get_template_name, template_id)
                return str(template_name)
            else:
                return None 

    def get_user(self, id):
        """ 
        Returns a user object given the user's ID

        Parameters
        ----------
        id : string
            The id/uuid of a user to check

        Returns
        -------
        A user object with the user's name, id, and email
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__get_user_by_id, id)

    def get_user_by_username(self, username):
        """ 
        Returns a user object given the user's username

        Parameters
        ----------
        username : string
            The username of a user to check

        Returns
        -------
        A user object with the user's name, id, and email
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__get_user_by_username, username)

    def get_users(self):
        """ 
        Returns a list of all users in the database

        Returns
        -------
        A list containing user ids
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_users)

    def get_visible_fields(self, dataset_id, user_id):
        """ 
        Returns the visible fields of a dataset for a user

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to check
        user_id : string
            The user to check field access for

        Returns
        -------
        A list of element UUIDs representing the visible fields
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__read_visible_fields, dataset_id, user_id)

    def is_unrestricted(self, dataset_id):
        """ 
        Checks if a dataset has 'Full' template access for the public users

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to check

        Returns
        -------
        True if the template name of the public role is set to 'Full', False otherwise
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__is_unrestricted_for_user, dataset_id, 'public')
    
    def is_unrestricted_for_user(self, dataset_id, user_id):
        """ 
        Checks if a dataset has 'Full' template access for a given user

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to check
        user_id : string
            The id/uuid of the user to check access for

        Returns
        -------
        True if the template name of the user's role is set to 'Full', False otherwise
        """
        with self.driver.session() as session:
            return session.read_transaction(self.__is_unrestricted_for_user, dataset_id, user_id)

    def set_dataset_description(self, dataset_id, language, description):
        """ 
        Sets a description for a dataset in a given language

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to assign a description to
        language : string
            A two letter identifier for the language of the description
        description : string
            The description to be applied to the dataset
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_dataset_description, dataset_id, language, description)

    def set_dataset_name(self, dataset_id, dataset_name):
        """ 
        Sets the name of the dataset to the provided string

        Parameters
        ----------
        dataset_id : string
            The id/uuid of a dataset to assign a name to
        dataset_name : string
            The name to be applied to the dataset
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_dataset_name, dataset_id, dataset_name)
            
    def set_template_access(self, role_id, template_id):
        """ 
        Sets a 'uses_template' relationship between a given role and template for a dataset

        Parameters
        ----------
        role_id : string
            The id/uuid of a role to 
        language : string
            A two letter identifier for the language of the description
        """
        with self.driver.session() as session:
            session.write_transaction(self.__bind_role_to_template, role_id, template_id)

    # Used to set access for users to edit org settings on the landing page
    def set_admin_form_access(self, user_id, org_id):
        """ 
        Sets a 'serves' relationship between a user and organization which allows accessing the organization on the admin form

        Parameters
        ----------
        user_id : string
            The id/uuid of a user to provide access to
        org_id : string
            The id/uuid of an organization to provide access to
        """
        with self.driver.session() as session:
            session.write_transaction(self.__bind_user_to_org, user_id, org_id)

    def set_user_gid(self, id, gid):
        """ 
        Sets the GID (Google ID) of a user in the database

        Parameters
        ----------
        id : string
            The id/uuid of the user in the database
        gid : string
            The value to set the 'gid' (Google ID) field to
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_gid, id, gid)

    def set_user_username(self, id, username):
        """ 
        Sets the user's CKAN username in the database

        Parameters
        ----------
        id : string
            The id/uuid of the user in the database
        username : string
            The value to set the 'username' field to
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_gid, id, username)            

    def set_user_email(self, id, email):
        """ 
        Sets the email address of a user in the database

        Parameters
        ----------
        id : string
            The id/uuid of the user in the database
        email : string
            The value to set the 'email' field to
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_email, id, email)   

    def set_user_role(self, user_id, role_id):
        """ 
        Gives a user access to a role in the database

        Parameters
        ----------
        user_id : string
            The id/uuid of the user in the database
        role_id : string
            The id/uuid of the role in the database
        """
        with self.driver.session() as session:
            session.write_transaction(self.__bind_user_to_role, user_id, role_id)

    def set_visible_fields(self, template_id, whitelist):
        """ 
        Allows a given template to be able to see a dataset's fields provided in the whitelist

        Parameters
        ----------
        template_id : string
            The id/uuid of the template in the database
        whitelist : list
            A list of UUIDs of the fields in the database
        """
        with self.driver.session() as session:
            session.write_transaction(self.__bind_fields_to_template, template_id, whitelist)

    def set_organization_name(self, org_id, org_name):
        """ 
        Sets the name of an organization in the database

        Parameters
        ----------
        org_id : string
            The id/uuid of the organization in the database
        org_name : string
            The organization name to be set
        """
        with self.driver.session() as session:
            session.write_transaction(self.__set_organization_name, org_id, org_name)

    def set_full_access_to_datasets(self, role_id):
        """ 
        Sets the template access to 'Full' for the given role for all datasets

        Parameters
        ----------
        role_id : string
            The id/uuid of the role in the database
        """
        with self.driver.session() as session:
            # Get all datasets
            # Get their full template
            # For each template
            templates = session.read_transaction(self.__read_all_templates, "Full")
            for template in templates:
                session.write_transaction(self.__bind_role_to_template, role_id, template)


    @staticmethod
    def __get_dataset(tx, id):
        """ 
        Runs a query to return a dataset's ID if it exists in the database

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to retrieve

        Returns
        -------
        The dataset id if it exists, None if it does not
        """
        records = tx.run("MATCH (d:dataset {id:'"+id+"'}) return d.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_group(tx, id):
        """ 
        Runs a query to return a group's ID if it exists in the database

        Parameters
        ----------
        id : string
            The id/uuid of the group to retrieve

        Returns
        -------
        The group id if it exists, None if it does not
        """
        records = tx.run("MATCH (g:group {id:'"+id+"'}) return g.id as id")   
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_org_by_id(tx, id):
        """ 
        Runs a query to return an organization's name and id given the id

        Parameters
        ----------
        id : string
            The id/uuid of the organization to retrieve

        Returns
        -------
        An object with the organization id and name if it exists, None if it does not
        """
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) return o.id AS id, o.name AS name")      
        for record in records:
            return record    
        return None

    @staticmethod
    def __get_org_by_name(tx, name):
        """ 
        Runs a query to return an organization's name and id given the name

        Parameters
        ----------
        name : string
            The name of the organization to retrieve

        Returns
        -------
        An object with the organization id and name if it exists, None if it does not
        """
        records = tx.run("MATCH (o:organization {name:'"+name+"'}) return o.id AS id, o.name AS name")      
        for record in records:
            return record    
        return None

    @staticmethod
    def __get_private_dataset(tx, id):
        """ 
        Given a dataset, runs a query to return the id of its related private dataset, if one exists

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to check for a related private dataset

        Returns
        -------
        An object with the id and name of the private dataset if it exists, None if it does not
        """
        records = tx.run("MATCH (x:dataset {id:'"+id+"'})<-[:has_public_dataset]-(y:dataset) return y.id as id, y.name as name")      
        for record in records:
            return record    
        return None
        
    @staticmethod
    def __get_public_dataset(tx, id):
        """ 
        Given a dataset, runs a query to return the id of its related public dataset, if one exists

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to check for a public private dataset

        Returns
        -------
        An object with the id and name of the public dataset if it exists, None if it does not
        """
        records = tx.run("MATCH (x:dataset {id:'"+id+"'})-[:has_public_dataset]->(y:dataset) return y.id as id, y.name as name")      
        for record in records:
            return record    
        return

    @staticmethod
    def __get_template_name(tx, template_id):
        """ 
        Given a template id, runs a query to return the name of the template

        Parameters
        ----------
        template_id : string
            The id/uuid of the template to get a name of

        Returns
        -------
        The template name as a string if the dataset exists, None if it does not
        """
        records = tx.run("MATCH (t:template {id:'"+template_id+"'}) return t.name AS name")
        for record in records:
            return record['name']
        return None

    @staticmethod
    def __get_template_access_for_role(tx, dataset_id, role_id):
        """ 
        Given a dataset and role, returns the template level of access that role has to the dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset
        role_id : string
            The id/uuid of the role to check access for

        Returns
        -------
        The id of the template as a string if a relationship exists, None if it does not
        """
        records = tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)<-[:uses_template]-(:role {id:'"+role_id+"'}) return t.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_template_access_for_user(tx, dataset_id, user_id):
        """ 
        Given a dataset and user, returns the template level of access that user has to the dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset
        user_id : string
            The id/uuid of the role to check access for

        Returns
        -------
        The id of the template as a string if a relationship exists, None if it does not
        """
        records = tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)<-[:uses_template]-(:role)<-[:has_role]-(u:user {id:'"+user_id+"'}) return t.id as id")
        for record in records:
            return record['id']

    @staticmethod 
    def __get_user_by_id(tx, id):
        """ 
        Runs a query to return an user's name and id given the id

        Parameters
        ----------
        id : string
            The id/uuid of the user to retrieve

        Returns
        -------
        An object with the user id and name if it exists, None if it does not
        """
        records = tx.run("MATCH (u:user {id:'"+id+"'}) return u.id as id, u.username as username, u.email as email")   
        for record in records:
            return record
        return None

    @staticmethod 
    def __get_user_by_username(tx, username):
        """ 
        Runs a query to return an user's name and id given the username

        Parameters
        ----------
        username : string
            The id/uuid of the user to retrieve

        Returns
        -------
        An object with the user id and name if it exists, None if it does not
        """
        records = tx.run("MATCH (u:user {username:'"+username+"'}) return u.id as id, u.username as username, u.email as email")   
        for record in records:
            return record
        return None

    @staticmethod
    def __is_unrestricted_for_user(tx, dataset_id, user_id):
        """ 
        Runs a query to check if the given user has full access to the dataset

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to check
        id : string
            The id/uuid of the user to check access for

        Returns
        -------
        True if the template access is named 'Full' and false if template access is any other form
        """
        records = tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)<-[:uses_template]-(:role)<-[:has_role]-(:user {id:'"+user_id+"'}) return t.name as name")
        for record in records:
            if record['name'] == 'Full':
                return True
            else:
                return False
        return True

    @staticmethod
    def __read_elements(tx, dataset_id):
        """ 
        Runs a query to retrieve all elements associated with a provided dataset

        Parameters
        ----------
        id : string
            The id/uuid of the user to retrieve

        Returns
        -------
        A dictionary of elements where each element name is the key and the id is the value
        """
        result = {}
        for record in tx.run("MATCH (:dataset {id:'"+dataset_id +"'})-[:has_template]->(t:template)-[:can_see]->(e:element) RETURN DISTINCT e.name AS name, e.id AS id"):
            result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_roles(tx, org_id=None):
        """ 
        Returns all roles in the database or roles managed by an organization if an id is provided

        Parameters
        ----------
        org_id : string
            The id/uuid of the organization to retrieve roles from (optional)

        Returns
        -------
        A dictionary of roles with the role name as the key and role id as the value.
        """
        result = {}
        # TODO This case is unused but may break if all roles are returned as many will share names
        # Possible to make a dictionary on the higher level to return per organization?
        if(org_id==None):
            for record in tx.run("MATCH (r:role) RETURN r.name AS name, r.id AS id"):
                result[record['name']] = record['id']
        else:
            for record in tx.run("MATCH (r:role)<-[:manages_role]-(o:organization {id:'"+ org_id +"'}) RETURN r.name AS name, r.id AS id"):
                result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_templates(tx, dataset_id):
        
        """ 
        Returns all templates in the database or templates related to a dataset if an id is provided

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to retrieve templates from (optional)

        Returns
        -------
        A dictionary of templates with the template name as the key and template id as the value.
        """
        result = {}
        for record in tx.run("MATCH (t:template)<-[:has_template]-(d:dataset {id:'"+ dataset_id +"'}) RETURN t.name AS name, t.id AS id"):
            result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_all_templates(tx, template_name=None):
        
        """ 
        Returns all templates in the database

        Parameters
        ----------
        template_name : string
            The name of the templates to return (optional)

        Returns
        -------
        A list of template IDs (String)
        """
        # TODO This case is unused but may break if all templates are returned as many will share names
        # Possible to make a dictionary on the higher level to return per dataset?
        results = []
        if (template_name==None):
            for record in tx.run("MATCH (t:template) RETURN t.id AS id"):
                results.append(record['id'])
        else:
            for record in tx.run("MATCH (t:template {name:'" + template_name + "'}) RETURN t.id AS id"):
                results.append(record['id'])
        return results

    @staticmethod
    def __read_users(tx):
        """ 
        Returns all users in the database

        Returns
        -------
        A list of all user ids in the database
        """
        result = []
        for record in tx.run("MATCH (u:user) RETURN u.id as id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __read_users_admins(tx):
        """ 
        Returns all CKAN sysadmin users in the database

        Returns
        -------
        A list of all user ids for CKAN sysadmins in the database
        """
        result = []
        for record in tx.run("MATCH (:role {id:'admin'})<-[:has_role]-(u:user) return u.id AS id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __read_visible_fields(tx, dataset_id, user_id):
        """ 
        Returns a list of all fields in a dataset that are accessible for the provided user

        Parameters
        ----------
        dataset_id : string
            The id/uuid of the dataset to check visible fields for
        user_id : string
            The id/uuid of the user to check permissions for

        Returns
        -------
        A list of element IDs that the user has access to
        """
        result = []
        for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:has_role]->(r:role)-[:uses_template]->(t:template)<-[:has_template]-(d:dataset {id:'"+dataset_id+"'}), (t)-[:can_see]->(e:element) return e.id AS id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __write_dataset(tx,id,dname=None):
        """ 
        Creates a dataset with the provided ID and name

        Parameters
        ----------
        id : string
            The id/uuid of the newly created dataset
        dname : string
            The name of the newly created dataset (optional)

        Returns
        -------
        None
        """
        if dname != None:
            # Create a safe dataset name if one is passed
            # https://stackoverflow.com/questions/7406102/create-sane-safe-filename-from-any-unsafe-string
            result = tx.run("CREATE (:dataset { id: '"+id+"', name:'"+"".join([c for c in dname if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'})")
        else:
            result = tx.run("CREATE (:dataset { id: '"+id+"'})")
        return

    @staticmethod
    def __write_group(tx, id):
        """ 
        Creates a group with the provided ID

        Parameters
        ----------
        id : string
            The id/uuid of the newly created group

        Returns
        -------
        None
        """
        result = tx.run("CREATE (g:group {id:'"+id+"'})")
        return

    @staticmethod
    def __write_metadata_field(tx, name, id, template_id):
        """ 
        Creates a metadata field/element with the provided ID and name and attached to the template_id
        The template_id provided should be the full template id for the dataset, as it is required to
            be linked to all metadata elements of the dataset and will keep a permanant link between the element and dataset
            as it cannot be erroneously deleted due to future template changes

        Parameters
        ----------
        name : string
            The name of the newly created element
        id : string
            The id/uuid of the newly created element
        template_id : string
            The id/uuid of the full template for the dataset that the element will be linked to

        Returns
        -------
        None
        """
        if name in constants.MINIMUM_FIELDS:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"',required:true})")
        else:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"'})")
        return

    @staticmethod
    def __write_org(tx, id, org_name=None):
        """ 
        Creates an organization with the provided ID and name

        Parameters
        ----------
        id : string
            The id/uuid of the newly created organization
        org_name : string
            The name of the newly created organization (optional)

        Returns
        -------
        None
        """
        if org_name != None:
            result = tx.run("CREATE (o:organization {id:'"+id+"', name:'"+str(org_name)+"'})")
        else:
            result = tx.run("CREATE (o:organization {id:'"+id+"'})")
        return

    @staticmethod
    def __write_role(tx, id, name=None):
        """ 
        Creates a role with the provided ID and name

        Parameters
        ----------
        id : string
            The id/uuid of the newly created role
        name : string
            The name of the newly created role (optional)

        Returns
        -------
        If the role id already exists, return the role id
        If the role does not already exist, returns None
        """
        records = tx.run("MATCH (r:role {id:'"+id+"'}) return r.id as id")
        for record in records:
            return record['id']
        if(name==None):
            tx.run("CREATE (r:role {id:'"+id+"'})")
        else:
            tx.run("CREATE (r:role {id:'"+id+"', name:'"+ str(name) +"'})")
        return None

    @staticmethod
    def __write_template(tx, id, name=None, description = None):
        """ 
        Creates a template with the provided ID, name, and description

        Parameters
        ----------
        id : string
            The id/uuid of the newly created template
        name : string
            The name of the newly created template (optional)
        description : string
            A description of the newly created template

        Returns
        -------
        If the template id already exists, returns the template id
        If the template does not exist, returns None
        """
        records = tx.run("MATCH (t:template {id:'"+id+"'}) return t.id AS id")
        for record in records:
            return record['id']
        properties = ""
        if(name!=None):
            properties+= ", name:'"+ str(name) +"'"
        if(description!=None):
            properties+= ", description:'"+ str(description) +"'"
        tx.run("CREATE (t:template {id:'"+id+ "'" + properties + "})")
        return None

    @staticmethod
    def __write_user(tx, id, username= None, email= None, gid = None):
        """ 
        Creates a user with the provided ID, username, email, and Google ID (gid).
        gid is required for the user to access the VITALITY API/Admin Form

        Parameters
        ----------
        id : string
            The id/uuid of the newly created user
        username : string
            The username of the newly created user in CKAN (optional)
        email : string
            The email of the newly created user (optional)
        gid : string
            The Google ID (gid) of the newly created user (optional)

        Returns
        -------
        If the user id already exists, returns the template user
        If the user does not exist, returns None
        """
        records = tx.run("MATCH (u:user {id:'"+id+"'}) return u.id AS id")
        for record in records:
            return record['id']
        extra_properties = ""
        if(username):
            extra_properties += ", username: '" + username + "'"
        if(email):
            extra_properties += ", email: '" + email +"'"
        if(gid):
            extra_properties += ", gid: '" + gid + "'"
        result = tx.run("CREATE (u:user {id:'"+id+"'" +extra_properties + "})")
        return

    #TODO Might be worth adding a check to see if the deletion objects actually delete/actually exist?

    @staticmethod
    def __delete_dataset(tx, id):
        """ 
        Deletes the dataset with the matching id as well as its associated templates and elements
        Deletion also removes any relationships associated with the dataset

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to delete

        Returns
        -------
        None
        """
        result = tx.run("MATCH (d:dataset {id: '"+id+"'})-[:has_template]->(t:template)-[:can_see]->(e:element) detach delete d,t,e")
        return
        
    
    #TODO: Remove member roles for the deleted organization? Or have a purge method for unassigned member roles
    @staticmethod
    def __delete_organization(tx, id):
        """ 
        Deletes the organization with the matching id
        Deletion also removes any relationships associated with the organization

        Parameters
        ----------
        id : string
            The id/uuid of the organization to delete

        Returns
        -------
        None
        """
        result = tx.run("MATCH (o:organization {id: '"+id+"'}) detach delete o")
        return

    @staticmethod
    def __delete_user(tx, id):
        """ 
        Deletes the user with the matching id
        Deletion also removes any relationships associated with the user

        Parameters
        ----------
        id : string
            The id/uuid of the user to delete

        Returns
        -------
        None
        """
        result = tx.run("MATCH (u:user {id: '"+id+"'}) detach delete u")
        return

    @staticmethod
    def __set_dataset_description(tx, id, language, description):
        """ 
        Sets the description field of a dataset with the given id
        The field that stores the description follows the format of description_{language} 
            e.g. for English the field would become description_en

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to set a description for
        language : string
            The language identifier to create a description field for (e.g. en, fr, sp)
        description : string
            The description to be added to the dataset

        Returns
        -------
        None
        """
        result = tx.run("MATCH (d:dataset {id: '"+id+"'}) set d.description_"+language+"='"+"".join([c for c in description if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __set_dataset_name(tx, id, name):
        """ 
        Sets the name of a dataset with the given id
        If the dataset already has a name it will be overwritten

        Parameters
        ----------
        id : string
            The id/uuid of the dataset to set a new name for
        name : string
            The new name for the dataset

        Returns
        -------
        None
        """
        result = tx.run("MATCH (d:dataset {id: '"+id+"'}) set d.name='"+"".join([c for c in name if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __set_user_gid(tx, id, gid):
        """ 
        Sets the Google ID (gid) of a user with the given id
        If the user already has a gid it will be overwritten

        Parameters
        ----------
        id : string
            The id/uuid of the user to set a new Google ID (gid) for
        gid : string
            The new Google ID (gid) of the user

        Returns
        -------
        None
        """
        tx.run("MATCH (u:user {id:'"+id+"'}) SET u.gid = '"+"".join([c for c in gid if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")   

    @staticmethod
    def __set_user_username(tx, id, username):
        """ 
        Sets the username of a user with the given id
        If the user already has a username it will be overwritten

        Parameters
        ----------
        id : string
            The id/uuid of the user to set a new username for
        username : string
            The new username of the user

        Returns
        -------
        None
        """
        tx.run("MATCH (u:user {id:'"+id+"'}) SET u.username = '"+"".join([c for c in username if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'") 

    @staticmethod
    def __set_user_email(tx, id, email):
        """ 
        Sets the email of a user with the given id
        If the user already has a email it will be overwritten

        Parameters
        ----------
        id : string
            The id/uuid of the user to set a new email for
        email : string
            The new email of the user

        Returns
        -------
        None
        """
        tx.run("MATCH (u:user {id:'"+id+"'}) SET u.email = '"+"".join([c for c in email if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")   


    @staticmethod
    def __set_organization_name(tx, id, name):
        """ 
        Runs a query to set the name of a given organization

        Parameters
        ----------
        id : string
            The id/uuid of the organization in the database
        name : string
            The value to set the 'name' field to
        """
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) set o.name ='"+"".join([c for c in name if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __bind_fields_to_template(tx, template_id, whitelist):  
        """ 
        Adds a list of elements to a template's visibility permissions
        Deletes all prior visibility relationships the template has

        Parameters
        ----------
        template_id : string
            The id/uuid of the template to set new visibility relationships for
        whitelist : list[string]
            A list of element IDs that will be visible to the template

        Returns
        -------
        None
        """
        # First remove all existing 'can_see' relationships between the template, dataset and its elements
        tx.run("MATCH (e:element)<-[c:can_see]-(t:template {id:'"+template_id+"'}) DELETE c")
        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(e)")
        return

    @staticmethod
    def __bind_dataset_to_org(tx, org_id, dataset_id):
        """ 
        Sets the ownership of a dataset to the specified organization
        If the dataset is already owned by a different organization, deletes the original owner relationship
            and creates the new one

        Parameters
        ----------
        org_id : string
            The id/uuid of the organization that will become responsible for the dataset
        dataset_id : string
            The id/uuid of the dataset that will become owned by the organization

        Returns
        -------
        If the specified organization already owns the dataset, returns the id of the relationship
        Otherwise, returns None
        """
        # Checks to see if relationship already exists
        records = tx.run("MATCH (o:organization {id:'"+org_id+"'})-[w:owns]->(d:dataset {id:'"+dataset_id+"'}) RETURN w")
        for record in records:
            return record['id']
        # Checks if dataset already owned, if so clears existing edge and adds new one
        tx.run("MATCH (d:dataset {id:'"+dataset_id+"'})<-[w:owns]-(o:organization) DELETE w")
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (d:dataset {id:'"+dataset_id+"'}) CREATE (o)-[:owns]->(d)")
        return

    @staticmethod
    def __bind_role_to_org(tx, role_id, org_id):
        """ 
        Sets a role to be associated with an organizaiton

        Parameters
        ----------
        role_id : string
            The id/uuid of the role that will be associated to an organization
        org_id : string
            The id/uuid of the organization that will be responsible for the provided role

        Returns
        -------
        None
        """
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (r:role {id:'"+role_id+"'}) CREATE (o)-[:manages_role]->(r)")
        return

    @staticmethod
    def __bind_role_to_template(tx, role_id, template_id):
        """ 
        Creates a relationship between a role and template so that the role can view elements visible to the template
        If the role already is associated with a template with the same dataset, the original relationship is deleted and
            a new one with the provided information is created

        Parameters
        ----------
        role_id : string
            The id/uuid of the role that will have access to the provided template
        template_id : string
            The id/uuid of the template that the role will be able to access

        Returns
        -------
        None
        """
        # Check to see if role already is connected to a template from that dataset, if so then delete edge
        # Can use this one without the dataset ID (unique role ids)
        records = tx.run("MATCH (r:role {id:'"+role_id+"'})-[u:uses_template]->(t:template {id:'"+template_id+"'}) RETURN u")
        for record in records:
            return
        tx.run("MATCH (t:template {id:'"+template_id+"'})<-[:has_template]-(d:dataset), (r:role {id:'"+role_id+"'})-[u:uses_template]->(:template)<-[:has_template]-(d) DELETE u")
        result = tx.run("MATCH (r:role {id:'"+role_id+"'}), (t:template {id:'"+template_id+"'}) CREATE (r)-[:uses_template]->(t)")
        return

    @staticmethod
    def __bind_template_to_dataset(tx, template_id, dataset_id):
        """ 
        Sets a template to be associated with a dataset
        If the template is already associated with a dataset, deletes the relationship between the original
            dataset and creates a new ownership with the provided dataset

        Parameters
        ----------
        template_id : string
            The id/uuid of the template that will be associated to the dataset
        dataset_id : string
            The id/uuid of the dataset that will be associated to the template

        Returns
        -------
        None
        """
        records = tx.run("MATCH (t:template {id:'"+template_id+"'})<-[h:has_template]-(d:dataset) RETURN h")
        for record in records:
            return
        tx.run("MATCH (t:template {id:'"+template_id+"'})<-[h:has_template]-(d:dataset {id:'"+dataset_id+"'}) DELETE h")
        result = tx.run("MATCH (t:template {id:'"+template_id+"'}), (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has_template]->(t)")
        return    

    @staticmethod
    def __bind_user_to_group(tx, group_id, user_id):
        """ 
        Sets a user to be associated with a specified group

        Parameters
        ----------
        user_id : string
            The id/uuid of the user that will be a member to the group
        group_id : string
            The id/uuid of the group that will have the user as a member

        Returns
        -------
        None
        """
        result = tx.run("MATCH (g:group {id:'"+group_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (g)-[:has_member]->(u)")
        return

    @staticmethod
    def __bind_user_to_org(tx, user_id, org_id):
        """ 
        Sets a user to be associated with a specified organization

        Parameters
        ----------
        user_id : string
            The id/uuid of the user that will serve the provided organization
        org_id : string
            The id/uuid of the org that will be managed by the provided user

        Returns
        -------
        None
        """
        # Not used in CKAN, but needed for Vitality admin form permissions
        tx.run("MATCH (u:user {id:'"+user_id+"'}), (o:organization {id:'"+org_id+"'}) CREATE (u)-[:serves]->(o)")
        return

    @staticmethod
    def __bind_user_to_role(tx, user_id, role_id):
        """ 
        Sets a user to have access to the provided role
        If the user already is assigned a role for the organization that manages the role,
            delete the relationship with the original role and create a new one for the 
            provided role

        Parameters
        ----------
        user_id : string
            The id/uuid of the user that will have access to the role
        role_id : string
            The id/uuid of the role that the user will have access to

        Returns
        -------
        None
        """
        # Check if edge already exists
        records = tx.run("MATCH (r:role {id:'"+role_id+"'})<-[h:has_role]-(u:user {id:'"+user_id+"'}) RETURN h")
        for record in records:
            return
        # Check to see if user is already given a role in the organization, and if so delete those edges
        tx.run("MATCH (r:role {id:'"+role_id+"'})<-[:manages_role]-(o:organization), (u:user {id:'"+user_id+"'})-[h:has_role]->(:role)<-[:manages_role]-(o) DELETE h")
        result = tx.run("MATCH (r:role {id:'"+role_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (u)-[:has_role]->(r)")
        return

    @staticmethod
    def __detach_user_from_role(tx, user_id, role_id):
        """ 
        Removes a user's access to a provided role

        Parameters
        ----------
        user_id : string
            The id/uuid of the user that will no longer have access to the role
        role_id : string
            The id/uuid of the role that the user will no longer have access to

        Returns
        -------
        None
        """
        tx.run("MATCH (r:role {id:'"+role_id+"'})<-[h:has_role]-(u:user {id:'"+user_id+"'}) delete h")
        return

    @staticmethod
    def __has_role(tx, user_id, role_id):
        """ 
        Checks if a user has access to a specific role

        Parameters
        ----------
        user_id : string
            The id/uuid of the user to check if they have access to the role
        role_id : string
            The id/uuid of the role to check if the user has access to

        Returns
        -------
        True if user has access to the role, False if they do not
        """
        # Checks if user has a specific role
        records = tx.run("MATCH (u:user {id:'"+user_id+"'})-[h:has_role]->(r:role {id:'"+role_id+"'}) RETURN h")
        for record in records:
            return True
        else:
            return False

if __name__ == "__main__":
    greeter = _GraphMetaAuth("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.__close()