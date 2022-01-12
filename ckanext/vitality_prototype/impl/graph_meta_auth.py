import logging
from os import stat
from ckanext.vitality_prototype.meta_authorize import MetaAuthorize
from neo4j import GraphDatabase
import uuid 
from ckanext.vitality_prototype import constants

log = logging.getLogger(__name__)

class _GraphMetaAuth(MetaAuthorize):
    """ Graph database authorization settings.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def __close(self):
        self.driver.close()

    def add_dataset(self, dataset_id, owner_id, dname=None):
        with self.driver.session() as session:
            # Check to see if the dataset already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_dataset, dataset_id) != None:
                return
            session.write_transaction(self.__write_dataset, dataset_id, dname)
            session.write_transaction(self.__bind_dataset_to_org, owner_id, dataset_id)

    def add_group(self, group_id, users):
        with self.driver.session() as session:
            # Check to see if the group already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_group, group_id):
                return
            session.write_transaction(self.__write_group, group_id)
            for user in users:
                session.write_transaction(self.__bind_user_to_group, group_id, user['id'])

    def add_metadata_fields(self, dataset_id, fields, template_id):
        with self.driver.session() as session:
            #TODO Update with templates
            # Get the names of the existing fields for this dataset
            log.info("adding extra fields")
            existing_fields = session.read_transaction(self.__read_elements, dataset_id)
            existing_names = [x[0] for x in existing_fields.items()]
            # For every new field to add
            for f in fields:
                log.info("%s adding field", f[0])
                # Only add the new field if a field with that name doesn't already exist
                if f[0] not in existing_names:
                    session.write_transaction(self.__write_metadata_field, f[0], str(f[1]), template_id)

    def add_org(self, org_id, users, org_name=None):
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
        with self.driver.session() as session:
            session.write_transaction(self.__write_role, id, name)

    def add_user(self, user_id, user_name = None, user_email = None):
        with self.driver.session() as session:
            # Check to see if the user already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_user_by_id, user_id) != None:
                return
            session.write_transaction(self.__write_user, user_id, user_name, user_email)

    def add_template(self, dataset_id, template_id, template_name=None, template_description=None):
        log.info('Creating a new template')
        with self.driver.session() as session:
            session.write_transaction(self.__write_template, template_id, template_name, template_description)
            session.write_transaction(self.__bind_template_to_dataset, template_id, dataset_id)        
    
    # Adds template and also adds the fields 
    # TODO Generate fields separately and set instead of two different instantiation methods
    def add_template_full(self, dataset_id, template_id, template_name, fields, template_description = None):
        with self.driver.session() as session:
            # Default templates already exist, skipping
            if session.read_transaction(self.__read_templates, dataset_id):
                log.info("templates exist already")
                return
            # Add full template
            self.add_template(dataset_id, template_id, template_name, template_description)
            # create the fields as well
            for name,id in fields.items():
                session.write_transaction(self.__write_metadata_field, name, id, template_id)


    def delete_organization(self, org_id):
        with self.driver.session() as session:
            session.write_transaction(self.__delete_organization, org_id)
            
    def delete_user(self, user_id):
        with self.driver.session() as session:
            session.write_transaction(self.__delete_user, user_id)

    def detach_user_role(self, user_id, role_id):
        with self.driver.session() as session:
            session.write_transaction(self.__detach_user_from_role, user_id, role_id)

    def get_admins(self):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_users_admins)

    def get_dataset(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__get_dataset, dataset_id)
            
    def get_metadata_fields(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_elements, dataset_id)

    def get_organization(self, organization_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__get_org_by_id, organization_id)

    def get_public_fields(self, dataset_id):
        public_field_ids =  self.get_visible_fields(dataset_id, user_id='public')
        public_field_names = [f[0].encode("utf-8") for f in self.get_metadata_fields(dataset_id).items() if f[1] in public_field_ids]
        return public_field_names

    def get_roles(self, org_id = None):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_roles, org_id)

    def get_templates(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_templates, dataset_id)    
            
    def get_template_access_for_role(self, dataset_id, role_id):
        with self.driver.session() as session:
            template_id = session.read_transaction(self.__get_template_access_for_role, dataset_id, role_id)
            if template_id != None:
                template_name = session.read_transaction(self.__get_template_name, template_id)
                return str(template_name)
            else:
                return None

    def get_template_access_for_user(self, dataset_id, user_id):
        with self.driver.session() as session:
            template_id = session.read_transaction(self.__get_template_access_for_user, dataset_id, user_id)
            if template_id != None:
                template_name = session.read_transaction(self.__get_template_name, template_id)
                return str(template_name)
            else:
                return None 

    def get_user(self, id):
        with self.driver.session() as session:
            return session.read_transaction(self.__get_user_by_id, id)

    def get_user_by_username(self, username):
        with self.driver.session() as session:
            return session.read_transaction(self.__get_user_by_username, username)

    def get_users(self):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_users)

    def get_visible_fields(self, dataset_id, user_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_visible_fields, dataset_id, user_id)

    def set_dataset_description(self, dataset_id, language, description):
        with self.driver.session() as session:
            session.write_transaction(self.__set_dataset_description, dataset_id, language, description)

    def set_dataset_name(self, dataset_id, dataset_name):
        with self.driver.session() as session:
            session.write_transaction(self.__set_dataset_name, dataset_id, dataset_name)

    def set_template_access(self, role_id, template_id):
        with self.driver.session() as session:
            session.write_transaction(self.__bind_role_to_template, role_id, template_id)

    # Used to set access for users to edit org settings on the landing page
    def set_admin_form_access(self, user_id, org_id):
        with self.driver.session() as session:
            session.write_transaction(self.__bind_user_to_org, user_id, org_id)

    def set_user_gid(self, id, gid):
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_gid, id, gid)

    def set_user_username(self, id, username):
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_gid, id, username)            

    def set_user_email(self, id, email):
        with self.driver.session() as session:
            session.write_transaction(self.__set_user_email, id, email)   

    def set_user_role(self, user_id, role_id):
        with self.driver.session() as session:
            session.write_transaction(self.__bind_user_to_role, user_id, role_id)

    def set_visible_fields(self, template_id, whitelist):
        with self.driver.session() as session:
            session.write_transaction(self.__bind_fields_to_template, template_id, whitelist)

    def set_organization_name(self, org_id, org_name):
        with self.driver.session() as session:
            session.write_transaction(self.__set_organization_name, org_id, org_name)


    @staticmethod
    def __set_organization_name(tx, id, name):
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) set o.name ='"+"".join([c for c in name if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __get_dataset(tx, id):
        records = tx.run("MATCH (d:dataset {id:'"+id+"'}) return d.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_group(tx, id):
        records = tx.run("MATCH (g:group {id:'"+id+"'}) return g.id as id")   
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_org_by_id(tx, id):
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) return o.id AS id, o.name AS name")      
        for record in records:
            return record    
        return None

    @staticmethod
    def __get_org_by_name(tx, name):
        records = tx.run("MATCH (o:organization {name:'"+name+"'}) return o.id AS id, o.name AS name")      
        for record in records:
            return record    
        return None

    @staticmethod
    def __get_template_name(tx, template_id):
        records = tx.run("MATCH (t:template {id:'"+template_id+"'}) return t.name AS name")
        for record in records:
            return record['name']

    @staticmethod
    def __get_template_access_for_role(tx, dataset_id, role_id):
        records = tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)<-[:uses_template]-(:role {id:'"+role_id+"'}) return t.id as id")
        for record in records:
            return record['id']

    @staticmethod
    def __get_template_access_for_user(tx, dataset_id, user_id):
        records = tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)<-[:uses_template]-(:role)<-[:has_role]-(u:user {id:'"+user_id+"'}) return t.id as id")
        for record in records:
            return record['id']

    @staticmethod 
    def __get_user_by_id(tx, id):
        records = tx.run("MATCH (u:user {id:'"+id+"'}) return u.id as id, u.username as username, u.email as email")   
        for record in records:
            return record
        return None

    @staticmethod 
    def __get_user_by_username(tx, username):
        records = tx.run("MATCH (u:user {username:'"+username+"'}) return u.id as id, u.username as username, u.email as email")   
        for record in records:
            return record
        return None

    @staticmethod
    def __read_elements(tx, dataset_id):
        #log.debug("Getting elements for dataset: %s", dataset_id)
        result = {}
        for record in tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has_template]->(t:template)-[:can_see]->(e:element) RETURN DISTINCT e.name AS name, e.id AS id"):
            #log.debug("record: %s", str(record))
            result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_roles(tx, org_id=None):
        """
        Returns all roles managed by the provided organization
        If no org_id provided, returns all roles
        """
        result = {}
        if(org_id==None):
            for record in tx.run("MATCH (r:role) RETURN r.name AS name, r.id AS id"):
                result[record['name']] = record['id']
        else:
            for record in tx.run("MATCH (r:role)<-[:manages_role]-(o:organization {id:'"+ org_id +"'}) RETURN r.name AS name, r.id AS id"):
                result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_templates(tx, dataset_id=None):
        result = {}
        if(dataset_id==None):
            for record in tx.run("MATCH (t:template) RETURN t.name AS name, t.id AS id"):
                result[record['name']] = record['id']
        else:
            for record in tx.run("MATCH (t:template)<-[:has_template]-(d:dataset {id:'"+ dataset_id +"'}) RETURN t.name AS name, t.id AS id"):
                result[record['name']] = record['id']
        return result

    @staticmethod
    def __read_users(tx):
        result = []
        for record in tx.run("MATCH (u:user) RETURN u.id as id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __read_users_admins(tx):
        result = []
        for record in tx.run("MATCH (:role {id:'admin'})<-[:has_role]-(u:user) return u.id AS id"):
            result.append(record['id'])
        log.info(result)
        return result

    @staticmethod
    def __read_visible_fields(tx, dataset_id, user_id):
        result = []
        for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:has_role]->(r:role)-[:uses_template]->(t:template)<-[:has_template]-(d:dataset {id:'"+dataset_id+"'}), (t)-[:can_see]->(e:element) return e.id AS id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __write_dataset(tx,id,dname=None):
        if dname != None:
            # Create a safe dataset name if one is passed
            # https://stackoverflow.com/questions/7406102/create-sane-safe-filename-from-any-unsafe-string
            result = tx.run("CREATE (:dataset { id: '"+id+"', name:'"+"".join([c for c in dname if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'})")
        else:
            result = tx.run("CREATE (:dataset { id: '"+id+"'})")
        return

    @staticmethod
    def __write_group(tx, id):
        result = tx.run("CREATE (g:group {id:'"+id+"'})")
        return

    @staticmethod
    def __write_metadata_field(tx, name, id, template_id):
        if name in constants.MINIMUM_FIELDS:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"',required:true})")
        else:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"'})")
        return

    @staticmethod
    def __write_org(tx, id, org_name=None):
        if org_name != None:
            result = tx.run("CREATE (o:organization {id:'"+id+"', name:'"+str(org_name)+"'})")
        else:
            result = tx.run("CREATE (o:organization {id:'"+id+"'})")
        return

    @staticmethod
    def __write_role(tx, id, name=None):
        records = tx.run("MATCH (r:role {id:'"+id+"'}) return r")
        for record in records:
            return record['id']
        if(name==None):
            tx.run("CREATE (r:role {id:'"+id+"'})")
        else:
            tx.run("CREATE (r:role {id:'"+id+"', name:'"+ str(name) +"'})")
        return None

    @staticmethod
    def __write_template(tx, id, name=None, description = None):
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
    def __write_user(tx, id, username= None, email= None):
        extra_properties = ""
        if(username):
            extra_properties += ", username: '" + username + "'"
        if(email):
            extra_properties += ", email: '" + email +"'"
        result = tx.run("CREATE (u:user {id:'"+id+"'" +extra_properties + "})")
        return

        
    @staticmethod
    def __delete_organization(tx, id):
        result = tx.run("MATCH (o:organization {id: '"+id+"'}) detach delete o")
        return

    @staticmethod
    def __delete_user(tx, id):
        result = tx.run("MATCH (u:user {id: '"+id+"'}) detach delete u")
        return

    @staticmethod
    def __set_dataset_description(tx, id, language, description):
        result = tx.run("MATCH (d:dataset {id: '"+id+"'}) set d.description_"+language+"='"+"".join([c for c in description if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __set_dataset_name(tx, id, name):
        result = tx.run("MATCH (d:dataset {id: '"+id+"'}) set d.name='"+"".join([c for c in name if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")
        return

    @staticmethod
    def __set_user_gid(tx, user_id, gid):
        tx.run("MATCH (u:user {id:'"+user_id+"'}) SET u.gid = '"+"".join([c for c in gid if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")   

    @staticmethod
    def __set_user_username(tx, id, username):
        tx.run("MATCH (u:user {id:'"+id+"'}) SET u.username = '"+"".join([c for c in username if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'") 

    @staticmethod
    def __set_user_email(tx, id, email):
        tx.run("MATCH (u:user {id:'"+id+"'}) SET u.email = '"+"".join([c for c in email if c.isalpha() or c.isdigit() or c==' ']).rstrip()+"'")   

    @staticmethod
    def __bind_fields_to_template(tx, template_id, whitelist):  
        # First remove all existing 'can_see' relationships between the template, dataset and its elements
        tx.run("MATCH (e:element)<-[c:can_see]-(t:template {id:'"+template_id+"'}) DELETE c")
        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(e)")
        return

    @staticmethod
    def __bind_dataset_to_org(tx, org_id, dataset_id):
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
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (r:role {id:'"+role_id+"'}) CREATE (o)-[:manages_role]->(r)")
        return

    @staticmethod
    def __bind_role_to_template(tx, role_id, template_id):
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
        records = tx.run("MATCH (t:template {id:'"+template_id+"'})<-[h:has_template]-(d:dataset) RETURN h")
        for record in records:
            return
        tx.run("MATCH (t:template {id:'"+template_id+"'})<-[h:has_template]-(d:dataset {id:'"+dataset_id+"'}) DELETE h")
        result = tx.run("MATCH (t:template {id:'"+template_id+"'}), (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has_template]->(t)")
        return    

    @staticmethod
    def __bind_user_to_group(tx, group_id, user_id):
        result = tx.run("MATCH (g:group {id:'"+group_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (g)-[:has_member]->(u)")
        return

    @staticmethod
    def __bind_user_to_org(tx, user_id, org_id):
        tx.run("MATCH (u:user {id:'"+user_id+"'}), (o:organization {id:'"+org_id+"'}) CREATE (u)-[:serves]->(o)")
        return

    @staticmethod
    def __bind_user_to_role(tx, user_id, role_id):
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
        tx.run("MATCH (r:role {id:'"+role_id+"'})<-[h:has_role]-(u:user {id:'"+user_id+"'}) delete h")
        return

    @staticmethod
    def __has_role(tx, user_id, role_id):
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