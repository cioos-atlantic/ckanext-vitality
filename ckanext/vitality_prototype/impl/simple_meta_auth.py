import pickle
import json
import logging
import os.path
from os import path

from ckanext.vitality_prototype.meta_authorize import MetaAuthorize

log = logging.getLogger(__name__)

class SimpleMetaAuth(MetaAuthorize):

    '''
    A dict of dataset ids and their fields
    '''
    dataset_fields = {}

    '''
    A ledger of whitelists:
        dataset_id -> user_id -> dict of visible fields
    '''
    ledger = {}


    '''
    A list of all known user ids
    '''
    users = []

    '''
    A list of all known group ids
    '''
    groups = []

    '''
    A list of all known org ids
    '''
    orgs = []

    '''
    NOTE: Groups and Orgs don't do anything in Simple Meta Auth, they're not even persisted.
    '''

    def add_group(self, group_id):
        if group_id not in self.groups:
            self.groups.append(group_id)

    def get_groups(self):
        return self.groups

    def add_org(self, org_id):
        if org_id not in self.orgs:
            self.orgs.append(org_id)
    
    def get_orgs(self):
        return self.orgs

    def add_user(self, user_id):
        log.info("Adding user " + str(user_id))
        log.info("Authorization model has " + str(len(self.users)) + " users")
        if user_id not in self.users:
            self.users.append(user_id)
        self.save()
    
    def get_users(self):
        return self.users

    def get_metadata_fields(self, dataset_id):
        log.info("dataset roster size: " + str(len(self.dataset_fields)))
        log.info("ledger size: " + str(len(self.ledger)))

        return self.dataset_fields[dataset_id]

    def get_visible_fields(self, dataset_id, user_id):
        return self.ledger[dataset_id][user_id].values()

    def add_dataset(self, dataset_id, fields):
        log.info("Adding dataset " + dataset_id + " with "+ str(len(fields)) +" fields to authorization model")
        self.dataset_fields[dataset_id] = fields
        self.save()


    def set_visible_fields(self, dataset_id, user_id, whitelist):
        # Create the dataset entry if it does not exist
        if dataset_id  not in self.ledger:
            self.ledger[dataset_id] = {}
        self.ledger[dataset_id][user_id] = whitelist
        self.save()

    def save(self):
        f_dataset = open("simple_auth_model_datasets.json","wb")
        json.dump(self.dataset_fields, f_dataset)
        f_dataset.flush()
        f_dataset.close()

        f_ledger = open("simple_auth_model_ledger.json", "wb")
        json.dump(self.ledger, f_ledger)
        f_ledger.flush()
        f_ledger.close()

        f_users = open("simple_auth_model_users.json", "wb")
        json.dump(self.users,f_users)
        f_users.flush()
        f_users.close()

    def load(self):

        if path.exists("simple_auth_model_datasets.json"):
            f_dataset = open("simple_auth_model_datasets.json","rb")
            self.dataset_fields = json.load(f_dataset)
            f_dataset.close()

        if path.exists("simple_auth_model_ledger.json"):
            f_ledger = open("simple_auth_model_ledger.json", "rb")
            self.ledger = json.load(f_ledger)
            f_ledger.close()

        if path.exists("simple_auth_model_users.json"):
            f_users = open("simple_auth_model_users.json", "rb")
            self.users = json.load(f_users)
            f_users.close()


