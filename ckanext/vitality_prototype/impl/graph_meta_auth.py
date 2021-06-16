import logging
from os import stat
from ckanext.vitality_prototype.meta_authorize import MetaAuthorize
from neo4j import GraphDatabase
import uuid 

log = logging.getLogger(__name__)

class GraphMetaAuth(MetaAuthorize):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        
    def close(self):
        self.driver.close()

    def add_org(self, org_id):
        with self.driver.session as session:
            session.write_transaction(self._write_org, org_id)

    def add_group(self, group_id):
        with self.driver.session() as session:
            session.write_transaction(self._write_group, group_id)

    def add_user(self, user_id):
        with self.driver.session() as session:
            session.write_transaction(self._write_user, user_id)

    def get_users(self):
        with self.driver.session() as session:
            return session.read_transaction(self._read_users)

    def add_dataset(self, dataset_id, fields):
        
        with self.driver.session() as session:

            session.write_transaction(self._write_dataset, dataset_id)

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
        result = tx.run("CREATE (o:org {id:'"+id+"'})")
        return

    @staticmethod
    def _write_group(tx, id):
        result = tx.run("CREATE (g:group {id:'"+id+"'}")
        return

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
        log.debug("Getting elements for dataset: %s", dataset_id)
        result = {}
        for record in tx.run("MATCH (:dataset {id:'"+dataset_id+"'})-[:has]->(e:element) RETURN e.name AS name, e.id AS id"):
            log.debug("record: %s", str(record))
            result[record['name']] = record['id']
        
        return result

    



if __name__ == "__main__":
    greeter = GraphMetaAuth("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.close()