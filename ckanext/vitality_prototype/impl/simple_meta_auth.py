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

    def load(self):

        if path.exists("simple_auth_model_datasets.json"):
            f_dataset = open("simple_auth_model_datasets.json","rb")
            self.dataset_fields = json.load(f_dataset)
            f_dataset.close()

        if path.exists("simple_auth_model_ledger.json"):
            f_ledger = open("simple_auth_model_ledger.json", "rb")
            self.ledger = json.load(f_ledger)
            f_ledger.close()

