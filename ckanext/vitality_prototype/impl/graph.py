
from ckanext.vitality_prototype.meta_authorize import MetaAuthorize
from neo4j import GraphDatabase
import uuid 

class Graph(MetaAuthorize):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    def index_pkg(self, pkg_dict):
        with self.driver.session() as session:

            #Create a dummy name/id for the dataset
            dataset = str(uuid.uuid4())
            session.write_transaction(self._write_dataset, dataset, dataset)

            for key in pkg_dict.keys():
                session.write_transaction(self._write_metadata_field, key, dataset)

    @staticmethod
    def _write_dataset(tx, name,id):
        result = tx.run("CREATE (:dataset {name:'"+name+"', id: '"+id+"'})")
        return

    @staticmethod
    def _write_metadata_field(tx, field, dataset_id):
        result = tx.run("MATCH (d:dataset {id:'"+dataset_id+"'}) CREATE (d)-[:has]->(:element {name:'"+field+"'})")

        return

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]


if __name__ == "__main__":
    greeter = Graph("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.close()