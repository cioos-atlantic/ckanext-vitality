from ckanext.vitality_prototype.impl.graph_meta_auth import GraphMetaAuth
import sys

from ckan import model
from ckan.common import config
from ckan.logic import get_action

from ckantoolkit import CkanCommand

class VitalityModel(CkanCommand):
    """Utility commands to manage the vitality metadata authorization model.

    Usage: 

        vitality seed_users
            - Loads all users from CKAN into the authorization model

    """

    summary = __doc__.split("\n")[0]
    usage = __doc__

    # Authorization Interface
    meta_authorize = None

    def __init__(self, name):
        super(VitalityModel, self).__init__(name)

    def command(self):
        self._load_config()

        # Load neo4j connection parameters from config
        neo4j_host = config.get('ckan.vitality.neo4j.host', "bolt://localhost:7687")
        neo4j_user = config.get('ckan.vitality.neo4j.user', "neo4j")
        neo4j_pass = config.get('ckan.vitality.neo4j.password', "password")
        # Initalizse meta_authorize
        self.meta_authorize = GraphMetaAuth(neo4j_host, neo4j_user, neo4j_pass)

        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)

        context = {
            "model": model,
            "session": model.Session,
            "ignore_auth": True
        }
        self.admin_user = get_action("get_site_user")(context, {})

        print("Hello world command!")

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)

        cmd = self.args[0]
        if cmd == "seed_users":
            self.seed_users(context)





    def seed_users(self, context):
        user_list = get_action('user_list')(context,{})

        print("Got {} users".format(len(user_list)))
        for u in user_list:
            print(u)
            self.meta_authorize.add_user(u['id'].decode('utf-8'))

        # Create the public user for people not logged in.
        self.meta_authorize.add_user('public')


    def _load_config(self):
        super(VitalityModel, self)._load_config()