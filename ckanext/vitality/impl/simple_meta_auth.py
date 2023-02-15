import json
import logging
import os

from ckanext.vitality.meta_authorize import MetaAuthorize

log = logging.getLogger(__name__)

class _SimpleMetaAuth(MetaAuthorize):

    '''
    A dict of dataset ids and their fields
    '''
    __dataset_fields = {}

    '''
    A ledger of whitelists:
        dataset_id -> user_id -> dict of visible fields
    '''
    __ledger = {}


    '''
    A list of all known user ids
    '''
    __users = []

    '''
    A list of all known group ids
    '''
    __groups = []

    '''
    A list of all known org ids
    '''
    __orgs = []

    '''
    NOTE: Groups and Orgs don't do anything in Simple Meta Auth, they're not even persisted.
    '''

    def add_group(self, group_id):
        if group_id not in self.__groups:
            self.__groups.append(group_id)

    def get_groups(self):
        return self.__groups

    def add_org(self, org_id):
        if org_id not in self.__orgs:
            self.__orgs.append(org_id)
    
    def get_orgs(self):
        return self.__orgs

    def add_user(self, user_id):
        log.info("Adding user " + str(user_id))
        log.info("Authorization model has " + str(len(self.__users)) + " users")
        if user_id not in self.__users:
            self.__users.append(user_id)
        self.__save()
    
    def get_users(self):
        return self.__users

    def get_metadata_fields(self, dataset_id):
        log.info("dataset roster size: " + str(len(self.__dataset_fields)))
        log.info("ledger size: " + str(len(self.__ledger)))

        return self.__dataset_fields[dataset_id]

    def get_visible_fields(self, dataset_id, user_id):
        return self.__ledger[dataset_id][user_id].values()

    def add_dataset(self, dataset_id, fields):
        log.info("Adding dataset " + dataset_id + " with "+ str(len(fields)) +" fields to authorization model")
        self.__dataset_fields[dataset_id] = fields
        self.__save()

    def set_visible_fields(self, dataset_id, user_id, whitelist):
        # Create the dataset entry if it does not exist
        if dataset_id  not in self.__ledger:
            self.__ledger[dataset_id] = {}
        self.__ledger[dataset_id][user_id] = whitelist
        self.__save()

    def __save(self):
        f_dataset = open("simple_auth_model_datasets.json","wb")
        json.dump(self.__dataset_fields, f_dataset)
        f_dataset.flush()
        f_dataset.close()

        f_ledger = open("simple_auth_model_ledger.json", "wb")
        json.dump(self.__ledger, f_ledger)
        f_ledger.flush()
        f_ledger.close()

        f_users = open("simple_auth_model_users.json", "wb")
        json.dump(self.__users,f_users)
        f_users.flush()
        f_users.close()

    def __load(self):

        if os.path.exists("simple_auth_model_datasets.json"):
            f_dataset = open("simple_auth_model_datasets.json","rb")
            self.__dataset_fields = json.load(f_dataset)
            f_dataset.close()

        if os.path.exists("simple_auth_model_ledger.json"):
            f_ledger = open("simple_auth_model_ledger.json", "rb")
            self.__ledger = json.load(f_ledger)
            f_ledger.close()

        if os.path.exists("simple_auth_model_users.json"):
            f_users = open("simple_auth_model_users.json", "rb")
            self.__users = json.load(f_users)
            f_users.close()