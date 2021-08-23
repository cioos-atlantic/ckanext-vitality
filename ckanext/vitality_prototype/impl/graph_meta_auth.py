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

    def add_dataset(self, dataset_id, fields, owner_id, dname=None):
        
        with self.driver.session() as session:

            # Check to see if the dataset already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self.__get_dataset, dataset_id) != None:
                return

            session.write_transaction(self.__write_dataset, dataset_id, dname)
            session.write_transaction(self.__bind_dataset_to_org, owner_id, dataset_id)
            # create the fields as well
            for name,id in fields.items():
                session.write_transaction(self.__write_metadata_field, name, id, dataset_id)

    def get_metadata_fields(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_elements, dataset_id)

    def get_visible_fields(self, dataset_id, user_id):
        with self.driver.session() as session:
            return session.read_transaction(self.__read_visible_fields, dataset_id, user_id)

    def set_visible_fields(self, dataset_id, user_id, whitelist):
        with self.driver.session() as session:
            session.write_transaction(self.__write_visible_fields, dataset_id, user_id, whitelist)

    def get_public_fields(self, dataset_id):
        public_field_ids =  self.get_visible_fields(dataset_id, user_id='public')

        public_field_names = [f[0].encode("utf-8") for f in self.get_metadata_fields(dataset_id).items() if f[1] in public_field_ids]

        #log.info("public field names:")
        #log.info(public_field_names)

        return public_field_names



    @staticmethod
    def __write_visible_fields(tx, dataset_id, user_id, whitelist):  
        # First remove all existing 'can_see' relationships between this user, dataset and its elements
        tx.run("MATCH (u:user {id:'"+user_id+"'})-[r:can_see]->(e:element)<-[:has]-(d:dataset {id:'"+dataset_id+"'}) DELETE r")

        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (u:user {id:'"+user_id+"'}) CREATE (u)-[:can_see]->(e)")
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
    def __write_metadata_field(tx, name, id, dataset_id):
        if name in constants.MINIMUM_FIELDS:
            result = tx.run("MATCH (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has]->(:element {name:'"+name+"',id:'"+id+"',required:true})")
        else:
            result = tx.run("MATCH (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has]->(:element {name:'"+name+"',id:'"+id+"'})")
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
    def __bind_user_to_org(tx, org_id, user_id):
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (o)-[:has_member]->(u)")
        return

    @staticmethod
    def __bind_user_to_group(tx, group_id, user_id):
        result = tx.run("MATCH (g:group {id:'"+group_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (g)-[:has_member]->(u)")
        return

    @staticmethod
    def __bind_dataset_to_org(tx, org_id, dataset_id):
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
    def __read_users(tx):
        result = []
        for record in tx.run("MATCH (u:user) RETURN u.id as id"):
            result.append(record['id'])

        return result

    @staticmethod
    def __read_visible_fields(tx, dataset_id, user_id):
        result = []
        for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:can_see]->(e:element)<-[:has]-(d:dataset {id:'"+dataset_id+"'}) return e.id AS id "):
            result.append(record['id'])
        return result

    @staticmethod
    def __read_elements(tx, dataset_id):
        #log.debug("Getting elements for dataset: %s", dataset_id)
        result = {}
        for record in tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has]->(e:element) RETURN e.name AS name, e.id AS id"):
            #log.debug("record: %s", str(record))
            result[record['name']] = record['id']
        return result

    @staticmethod
    def __create_role(tx, name):
        records = tx.run("MATCH (r:role {id:'"+name+"'}) return r")
        if len(records) > 0:
            return None
        else:
            tx.run("CREATE (r:role {id:'"+name+"'})")
        return None

    @staticmethod
    def __write_role_fields(tx, dataset_id, role, whitelist):  
        # First remove all existing 'can_see' relationships between this user, dataset and its elements
        tx.run("MATCH (r:role {id:'"+role+"'})-[c:can_see]->(e:element)<-[:has]-(d:dataset {id:'"+dataset_id+"'}) DELETE c")

        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (r:role {id:'"+role+"'}) CREATE (r)-[:can_see]->(e)")
        return

if __name__ == "__main__":
    greeter = _GraphMetaAuth("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.__close()