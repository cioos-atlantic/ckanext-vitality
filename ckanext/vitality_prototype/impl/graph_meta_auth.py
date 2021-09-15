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

    def add_org(self, org_id, users, org_name=None):
        with self.driver.session() as session:
            # Check to see if the org already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_org, org_id):
                return

            session.write_transaction(self.__write_org, org_id, org_name)

            # Create member role
            member_id = str(uuid.uuid4())
            session.write_transaction(self.__write_role, member_id, "Member")
            session.write_transaction(self.__bind_role_to_org, member_id, org_id)

            for user in users:
                session.write_transaction(self.__bind_user_to_org, org_id, user['id'])

    def add_group(self, group_id, users):
        with self.driver.session() as session:
            # Check to see if the group already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_group, group_id):
                return

            session.write_transaction(self.__write_group, group_id)

            for user in users:
                session.write_transaction(self.__bind_user_to_group, group_id, user['id'])

    def add_user(self, user_id):
        with self.driver.session() as session:
            # Check to see if the user already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_user, user_id) != None:
                return

            session.write_transaction(self.__write_user, user_id)
    
    def add_metadata_fields(self, dataset_id, fields):
        with self.driver.session() as session:
            #TODO Update with templates
            # Get the names of the existing fields for this dataset
            existing_fields = session.read_transaction(self.__read_elements, dataset_id)
            existing_names = [x[0] for x in existing_fields.items()]

            # For every new field to add
            for f in fields:
                # Only add the new field if a field with that name doesn't already exist
                if f[0] not in existing_names:
                    session.write_transaction(self.__write_metadata_field, f[0], str(f[1]), dataset_id)

    def get_users(self):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_users)

    def get_dataset(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__get_dataset, dataset_id)

    def add_role(self, id, name=None):
        with self.driver.session() as session:
            session.write_transaction(self.__write_role, id, name)

    def get_roles(self):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_roles)

    def add_dataset(self, dataset_id, owner_id, dname=None):
        with self.driver.session() as session:
            # Check to see if the dataset already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_dataset, dataset_id) != None:
                return

            session.write_transaction(self.__write_dataset, dataset_id, dname)
            session.write_transaction(self.__bind_dataset_to_org, owner_id, dataset_id)

    def add_full_template(self, dataset_id, template_id, template_name, fields):
        with self.driver.session() as session:
            # Default templates already exist, skipping
            if session.read_transaction(self.__read_templates, dataset_id):
                log.info("templates exist already")
                return

            # Add full template
            self.add_template(dataset_id, template_id, template_name)

            # create the fields as well
            for name,id in fields.items():
                session.write_transaction(self.__write_metadata_field, name, id, template_id)

    def add_template(self, dataset_id, template_id, template_name=None):
        with self.driver.session() as session:
            session.write_transaction(self.__write_template, template_id, template_name)
            session.write_transaction(self.__bind_template_to_dataset, template_id, dataset_id)

    def get_metadata_fields(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_elements, dataset_id)

    def get_visible_fields(self, dataset_id, user_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_visible_fields, dataset_id, user_id)

    def set_visible_fields(self, template_id, whitelist):
        with self.driver.session() as session:
            session.write_transaction(self.__write_visible_fields, template_id, whitelist)

    def get_public_fields(self, dataset_id):
        public_field_ids =  self.get_visible_fields(dataset_id, user_id='public')

        public_field_names = [f[0].encode("utf-8") for f in self.get_metadata_fields(dataset_id).items() if f[1] in public_field_ids]

        #log.info("public field names:")
        #log.info(public_field_names)

        return public_field_names

    def set_template_access(self, user_id, template_id):
        with self.driver.session() as session:
            session.write_transaction(self.__bind_user_to_template, user_id, template_id)

    @staticmethod
    def __write_visible_fields(tx, template_id, whitelist):  
        # First remove all existing 'can_see' relationships between the template, dataset and its elements
        tx.run("MATCH (e:element)<-[c:can_see]-(t:template {id:'"+template_id+"'}) DELETE c")

        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(e)")
        return

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
    def __write_metadata_field(tx, name, id, template_id):
        if name in constants.MINIMUM_FIELDS:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"',required:true})")
        else:
            result = tx.run("MATCH (t:template {id:'"+template_id+"'}) CREATE (t)-[:can_see]->(:element {name:'"+name+"',id:'"+id+"'})")
        return

    @staticmethod
    def __write_user(tx, id):
        result = tx.run("CREATE (u:user {id:'"+id+"'})")
        return

    @staticmethod
    def __write_org(tx, id, org_name=None):
        if org_name != None:
            result = tx.run("CREATE (o:organization {id:'"+id+"', name:'"+str(org_name)+"'})")
        else:
            result = tx.run("CREATE (o:organization {id:'"+id+"'})")
        return

    @staticmethod
    def __write_group(tx, id):
        result = tx.run("CREATE (g:group {id:'"+id+"'})")
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
    def __write_template(tx, id, name=None):
        records = tx.run("MATCH (t:template {id:'"+id+"'}) return t.id AS id")
        for record in records:
            return record['id']
        if(name==None):
            tx.run("CREATE (t:template {id:'"+id+"'})")
        else:
            tx.run("CREATE (t:template {id:'"+id+"', name:'"+ str(name) +"'})")
        return None

    @staticmethod
    def __bind_user_to_org(tx, org_id, user_id):
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (o)-[:has_member]->(u)")
        return

    @staticmethod
    def __bind_user_to_group(tx, group_id, user_id):
        result = tx.run("MATCH (g:group {id:'"+group_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (g)-[:has_member]->(u)")
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
    def __get_org(tx, id):
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) return o.id as id")      
        for record in records:
            return record['id']    
        return None

    @staticmethod 
    def __get_user(tx, id):
        records = tx.run("MATCH (u:user {id:'"+id+"'}) return u.id as id")   
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
    def __get_dataset(tx, id):
        records = tx.run("MATCH (d:dataset {id:'"+id+"'}) return d.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_role(tx, id):
        records = tx.run("MATCH (r:role {id:'"+id+"'}) return r.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __get_template(tx, id):
        records = tx.run("MATCH (t:template {id:'"+id+"'}) return t.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def __read_users(tx):
        result = []
        for record in tx.run("MATCH (u:user) RETURN u.id as id"):
            result.append(record['id'])
        return result

    @staticmethod
    def __read_visible_fields(tx, dataset_id, user_id):
        result = []
        #for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:has_role]->(r:role)-[:uses_template]->(t:template)<-[:has_template]-(d:dataset {id:'"+dataset_id+"'}), (t)-[:can_see]->(e:element) return e.id AS id "):
        for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:uses_template]->(t:template)<-[:has_template]-(d:dataset {id:'"+dataset_id+"'}), (t)-[:can_see]->(e:element) return e.id AS id "):
            result.append(record['id'])
        return result

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
            for record in tx.run("MATCH (r:role)<-[:has_role]-(o:organization {id:'"+ org_id +"'}) RETURN r.name AS name, r.id AS id"):
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
    def __bind_role_to_org(tx, role_id, org_id):
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (r:role {id:'"+role_id+"'}) CREATE (o)-[:has_role]->(r)")
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
    def __bind_user_to_template(tx, user_id, template_id):
        # TEMPORARY - To test templates without roles
        records = tx.run("MATCH (u:user {id:'"+user_id+"'})-[h:uses_template]->(t:template {id:'"+template_id+"'}) RETURN h")
        for record in records:
            return
        tx.run("MATCH (t:template {id:'"+template_id+"'})<-[:has_template]-(d:dataset), (u:user {id:'"+user_id+"'})-[h:uses_template]->(:template)<-[:has_template]-(d) DELETE h")
        result = tx.run("MATCH (u:user {id:'"+user_id+"'}), (t:template {id:'"+template_id+"'}) CREATE (u)-[:uses_template]->(t)")
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

if __name__ == "__main__":
    greeter = _GraphMetaAuth("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.__close()