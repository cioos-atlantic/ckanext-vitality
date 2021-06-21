import logging
from os import stat
from ckanext.vitality_prototype.meta_authorize import MetaAuthorize
from neo4j import GraphDatabase
import uuid 

log = logging.getLogger(__name__)

class GraphMetaAuth(MetaAuthorize):
    """ Graph database authorization settings.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()

    def add_org(self, org_id, users):
        with self.driver.session() as session:
            # Check to see if the org already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self._get_org, org_id):
                return

            session.write_transaction(self._write_org, org_id)

            for user in users:
                session.write_transaction(self._bind_user_to_org, org_id, user['id'])

    def add_group(self, group_id, users):
        with self.driver.session() as session:
            # Check to see if the group already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self._get_group, group_id):
                return

            session.write_transaction(self._write_group, group_id)

            for user in users:
                session.write_transaction(self._bind_user_to_group, group_id, user['id'])

    def add_user(self, user_id):
        with self.driver.session() as session:
            # Check to see if the user already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self._get_user, user_id) != None:
                return

            session.write_transaction(self._write_user, user_id)

    def get_users(self):
        with self.driver.session() as session:
            return session.read_transaction(self._read_users)

    def add_dataset(self, dataset_id, fields, owner_id):
        
        with self.driver.session() as session:

            # Check to see if the dataset already exists, if so we're done as we don't want to create duplicates.
            if session.read_transaction(self._get_dataset, dataset_id) != None:
                return

            session.write_transaction(self._write_dataset, dataset_id)
            session.write_transaction(self._bind_dataset_to_org, owner_id, dataset_id)
            # create the fields as well
            for name,id in fields.items():
                session.write_transaction(self._write_metadata_field, name, id, dataset_id)

    def get_metadata_fields(self, dataset_id):
        with self.driver.session() as session:
            return session.read_transaction(self._read_elements, dataset_id)

    def get_visible_fields(self, dataset_id, user_id):
        with self.driver.session() as session:
            return session.read_transaction(self._read_visible_fields, dataset_id, user_id)

    def set_visible_fields(self, dataset_id, user_id, whitelist):
        with self.driver.session() as session:
            session.write_transaction(self._write_visible_fields, dataset_id, user_id, whitelist)

    @staticmethod
    def _write_visible_fields(tx, dataset_id, user_id, whitelist):      
        for name,id in whitelist.items():
            result = tx.run("MATCH (e:element {id:'"+id+"'}), (u:user {id:'"+user_id+"'}) CREATE (u)-[:can_see]->(e)")
        return

    @staticmethod
    def _write_dataset(tx,id):
        result = tx.run("CREATE (:dataset { id: '"+id+"'})")
        return

    @staticmethod
    def _write_metadata_field(tx, name, id, dataset_id):
        result = tx.run("MATCH (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has]->(:element {name:'"+name+"',id:'"+id+"'})")
        return

    @staticmethod
    def _write_user(tx, id):
        result = tx.run("CREATE (u:user {id:'"+id+"'})")
        return

    @staticmethod
    def _write_org(tx, id):
        result = tx.run("CREATE (o:organization {id:'"+id+"'})")
        return

    @staticmethod
    def _write_group(tx, id):
        result = tx.run("CREATE (g:group {id:'"+id+"'})")
        return

    @staticmethod
    def _bind_user_to_org(tx, org_id, user_id):
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (o)-[:has_member]->(u)")
        return

    @staticmethod
    def _bind_user_to_group(tx, group_id, user_id):
        result = tx.run("MATCH (g:group {id:'"+group_id+"'}), (u:user {id:'"+user_id+"'}) CREATE (g)-[:has_member]->(u)")
        return

    @staticmethod
    def _bind_dataset_to_org(tx, org_id, dataset_id):
        result = tx.run("MATCH (o:organization {id:'"+org_id+"'}), (d:dataset {id:'"+dataset_id+"'}) CREATE (o)-[:owns]->(d)")
        return

    @staticmethod
    def _get_org(tx, id):
        records = tx.run("MATCH (o:organization {id:'"+id+"'}) return o.id as id")      
        for record in records:
            return record['id']    
        return None

    @staticmethod 
    def _get_user(tx, id):
        records = tx.run("MATCH (u:user {id:'"+id+"'}) return u.id as id")   
        for record in records:
            return record['id']
        return None
    
    @staticmethod
    def _get_group(tx, id):
        records = tx.run("MATCH (g:group {id:'"+id+"'}) return g.id as id")   
        for record in records:
            return record['id']
        return None

    @staticmethod
    def _get_dataset(tx, id):
        records = tx.run("MATCH (d:dataset {id:'"+id+"'}) return d.id as id")
        for record in records:
            return record['id']
        return None

    @staticmethod
    def _read_users(tx):
        result = []
        for record in tx.run("MATCH (u:user) RETURN u.id as id"):
            result.append(record['id'])

        return result

    @staticmethod
    def _read_visible_fields(tx, dataset_id, user_id):
        result = []
        for record in tx.run("MATCH (u:user {id:'"+user_id+"'})-[:can_see]->(e:element)<-[:has]-(d:dataset {id:'"+dataset_id+"'}) return e.id AS id"):
            result.append(record['id'])
        return result

    @staticmethod
    def _read_elements(tx, dataset_id):
        #log.debug("Getting elements for dataset: %s", dataset_id)
        result = {}
        for record in tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has]->(e:element) RETURN e.name AS name, e.id AS id"):
            #log.debug("record: %s", str(record))
            result[record['name']] = record['id']
        return result

if __name__ == "__main__":
    greeter = GraphMetaAuth("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.close()