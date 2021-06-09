import logging

log = logging.getLogger(__name__)

class MetaAuthorize(object):


    '''
    Add a dataset to the metadata authorization model
    '''
    def add_dataset(self, dataset_id, fields):
        raise NotImplementedError("Class %s doesn't implement add_dataset(self, dataset_id, fields)" % (self.__class__.__name__))

    '''
    Return a map of metadata fields with their UUID's for a particular dataset.
    '''
    def get_metadata_fields(self, dataset_id):
        raise NotImplementedError("Class %s doesn't implement get_metadata_fields(self, dataset_id)" % (self.__class__.__name__))


    '''
    Return the subset of metadata field ids in this dataset
    for which the given user has read access.
    '''
    def get_visible_fields(self, dataset_id, user_id):
        raise NotImplementedError("Class %s doesn't implement get_visible_fields(self, dataset_id, user_id)")


    '''
    Sets the visible fields for a user and a particular dataset
    '''
    def set_visible_fields(self, dataset_id, user_id, whitelist):
        raise NotImplementedError("Class %s doesn't implement set_visible_fields(self, dataset_id, user_id, whitelist)" % (self.__class__.__name__))


    '''
    Recursively filter a dictionary, returning only white-listed keys.
    
    input - the dictionary to filter
    fields - a dictionary of fields and ids the dictionary contains
    whitelist - a list of whitelisted ids, all other fields will be popped.

    Returns the filtered input
    '''
    def filter_dict(self, input, fields, whitelist):

        #log.info(fields)

        # Trivially check input type
        if type(input) != dict:
            raise TypeError("Only dicts can be filtered recursively! Attempted to filter " + str(type(input)))

        # Iterate through the dictionary entries
        for key,value in input.items():

            log.info("Checking authorization for " + str(key))

            # TODO - this needs to be handled way better
            if key == "en" or key == "fr":
                continue

            # Pop unknown fields
            if key.encode('utf-8') not in fields:
                input.pop(key, None)
                log.warn("Popped unknown field: " + str(key))
                continue

            # Get field id corresponding with key
            curr_field_id = fields[key.encode('utf-8')]

            # If the current field's id does not appear in the whitelist, pop it from the input
            if curr_field_id not in whitelist:
                input.pop(key, None)
                log.info("Key rejected!")
                continue

            # If the value is a dict, recurse 
            if type(value) is dict:
                
                # Overwrite value with filtered dict
                input[key] = self.filter_dict(value, fields, whitelist)
            
            log.info("Key authorized!")


        # log.info("Filtered input")
        # log.info(input)
                
        return input
