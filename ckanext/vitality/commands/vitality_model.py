"""
Depreciated for CKAN 2.9 and later. Use cli.py instead
"""
from ckanext.vitality.meta_authorize import MetaAuthorize, MetaAuthorizeType
import logging
import sys

# CKAN interfacing imports
from ckan import model
from ckan.common import config
from ckan.logic import get_action
from ckantoolkit import CkanCommand

CMD_ARG = 0
MIN_ARGS = 1


class VitalityModel(CkanCommand):
    """
    Utility commands to manage the vitality metadata authorization model.

    ...

    Attributes
    ----------
    meta_authorize: GraphMetaAuth
        Authorization information for Neo4J.

    Methods
    ------- 
    seed_users(context)
        Loads all users from CKAN into the authorization model and creates one 'public' user for anonymous access
        using the provided context object.

    seed_groups(context)
        Loads all groups from CKAN into the authorization model using the provided context object.

    seed_orgs(context)
        Loads all organizations from CKAN into the authorization model using the provided context object.
        
    seed(context)
        Loads users, groups, and organizations into the authorization model using the provided context object.
    """

    # Authorization Interface
    meta_authorize = None

    # Required by CKAN Commands
    summary = __doc__.split("\n")[0]
    usage = __doc__

    def __init__(self, name):
        super(VitalityModel, self).__init__(name)

    def command(self):
        """
        Ingests command line arguments 
        """
        self._load_config()

        # Load neo4j connection parameters from config
        # Initalize meta_authorize
        self.meta_authorize = MetaAuthorize.create(MetaAuthorizeType.GRAPH, {
            'host': config.get('ckan.vitality.neo4j.host', "bolt://localhost:7687"),
            'user': config.get('ckan.vitality.neo4j.user', "neo4j"),
            'password': config.get('ckan.vitality.neo4j.password', "password")
        })

        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)

        context = {
            "model": model,
            "session": model.Session,
            "ignore_auth": True
        }
        self.admin_user = get_action("get_site_user")(context, {})

        if len(self.args) < MIN_ARGS:
            print("No args!")
            self.parser.print_usage()
            sys.exit(1)

        cmd = self.args[CMD_ARG]
        print("cmd: {}".format(cmd))
        
        if cmd == "seed_users":
            self.seed_users(context)
        elif cmd == "seed_groups":
            self.seed_groups(context)
        elif cmd == "seed_orgs":
            self.seed_orgs(context)
        elif cmd == "seed":
            self.seed_users(context)
            self.seed_groups(context)
            self.seed_orgs(context)


    def seed_orgs(self, context):
        org_list = get_action('organization_list')(context, {'all_fields':True, 'include_users':True})
        print("Got {} organizations".format(len(org_list)))
        print(org_list)

        admin_list = self.meta_authorize.get_admins()

        # Set admin access for each organization
        for o in org_list:
            self.meta_authorize.add_org(o['id'],o['users'],o['name'])
            for admin in admin_list:
                self.meta_authorize.set_admin_form_access(admin, o['id'])
            continue

    def seed_groups(self, context):
        group_list = get_action('group_list')(context,{'all_fields':True, 'include_users':True})
        print("Got {} groups".format(len(group_list)))

        for g in group_list:
            self.meta_authorize.add_group(g['id'].decode('utf-8'), g['users'])

        print("group_list")
        print(group_list)


    def seed_users(self, context):
        # Create admin role
        self.meta_authorize.add_role('admin', 'admin')
        self.meta_authorize.add_role('public', 'public')

        user_list = get_action('user_list')(context,{})
        print("Got {} users".format(len(user_list)))
        print(user_list)
        for u in user_list:
            print(u.decode('utf-8'))

            user_id = u['id'].decode('utf-8')
            user_name = u['name'].decode('utf-8')
            user_email = ""
            gid = ""

            # Email not required, so check if it exists first
            if(u['email']):
                user_email = u['email'].decode('utf-8')
            self.meta_authorize.add_user(user_id, user_name, user_email,gid)

            # Admins in CKAN are marked as such
            if u['sysadmin']:
                self.meta_authorize.set_user_role(user_id, 'admin')

        # Create the public user & role for people not logged in.
        self.meta_authorize.add_user('public', 'Public')
        self.meta_authorize.set_user_role('public', 'public')

    def set_all_datasets_public(self, context):
        self.meta_authorize.set_full_access_to_datasets("public")

    def _load_config(self):
        super(VitalityModel, self)._load_config()