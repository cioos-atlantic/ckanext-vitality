"""
Code primarily adapted from vitality_model.py, which was used with paster commands for earlier versions of ckan
"""
import click
from ckanext.vitality.meta_authorize import MetaAuthorize, MetaAuthorizeType
import logging
import sys

from flask import current_app as app

# CKAN interfacing imports
from ckan import model
from ckan.common import config
from ckan.logic import get_action

def get_commands():
    return[vitality]

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

CMD_ARG = 0
MIN_ARGS = 1

# Authorization Interface
meta_authorize = None

@click.group()
@click.pass_context
def vitality(ctx):
    '''For use with the Vitality commands'''
    # Load neo4j connection parameters from config
    # Initalize meta_authorize
    meta_authorize = MetaAuthorize.create(MetaAuthorizeType.GRAPH, {
        'host': config.get('ckan.vitality.neo4j.host', "bolt://localhost:7687"),
        'user': config.get('ckan.vitality.neo4j.user', "neo4j"),
        'password': config.get('ckan.vitality.neo4j.password', "password")
    })

    session = {
    "model": model,
    "session": model.Session,
    "ignore_auth": True
    }
    admin_user = get_action("get_site_user")(session, {})

    click.echo(admin_user)

    click.echo(type(meta_authorize))

    ctx.ensure_object(dict)

    ctx.obj['meta_authorize'] = meta_authorize
    ctx.obj['session'] = session

    click.echo('Vitality connection initialized')

    pass

@vitality.command()
@click.pass_context
def seed_users(ctx):
    # Create admin role
    ctx.obj['meta_authorize'].add_role('admin', 'admin')
    ctx.obj['meta_authorize'].add_role('public', 'public')

    user_list = get_action('user_list')(ctx.obj['session'],{})
    print("Got {} users".format(len(user_list)))
    for u in user_list:
        user_id = u['id']
        user_name = u['name']
        user_email = ""

        # Email not required, so check if it exists first
        if('email' in u):
            user_email = u['email']
        ctx.obj['meta_authorize'].add_user(user_id, user_name, user_email)

        # Admins in CKAN are marked as such
        if u['sysadmin']:
            ctx.obj['meta_authorize'].set_user_role(user_id, 'admin')

        # TODO Figure out a better way to set GIDs?
        ctx.obj['meta_authorize'].set_user_gid(user_id, "guest")

    # Create the public user & role for people not logged in.
    ctx.obj['meta_authorize'].add_user('public', 'Public')
    ctx.obj['meta_authorize'].set_user_role('public', 'public')
    return

@vitality.command()
@click.pass_context
def seed_groups(ctx):
    group_list = get_action('group_list')(ctx.obj['session'],{'all_fields':True, 'include_users':True})
    print("Got {} groups".format(len(group_list)))

    for g in group_list:
        ctx.obj['meta_authorize'].add_group(g['id'].decode('utf-8'), g['users'])
    return


@vitality.command()
@click.pass_context
def seed_orgs(ctx):
    print(ctx.obj['session'])
    org_list = get_action('organization_list')(ctx.obj['session'], {'all_fields':True,'include_users':True, 'include_extras':True})
    print("Got {} organizations".format(len(org_list)))

    admin_list = ctx.obj['meta_authorize'].get_admins()

    # Set admin access for each organization
    for o in org_list:
        ctx.obj['meta_authorize'].add_org(o['id'],o['users'],o['name'])
        for admin in admin_list:
            ctx.obj['meta_authorize'].set_admin_form_access(admin, o['id'])
        continue
    return

@vitality.command()
@click.pass_context
def set_all_datasets_public(ctx):
    ctx.obj['meta_authorize'].set_full_access_to_datasets("public")
    return

@vitality.command()
@click.argument(u'dataset_id')
@click.pass_context 
def set_dataset_private(ctx, dataset_id):
    ctx.obj['meta_authorize'].set_minimal_access_to_dataset(dataset_id)
    return

@vitality.command()
@click.argument(u'dataset_id')
@click.argument(u'template_name')
@click.argument(u'element_name')
@click.pass_context 
def set_element_access_for_template(ctx, dataset_id, template_name, element_name):
    ctx.obj['meta_authorize'].set_element_access_for_template(dataset_id, template_name, element_name)