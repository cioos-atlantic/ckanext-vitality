import logging

log = logging.getLogger(__name__)

class MetaAuthorize(object):
    """ 
    An authorization tool for displaying metadata in CKAN

    ...

    Methods
    ------- 
    add_org(org_id)
        Adds a organization org_id to the authorization model.

    get_orgs()
        Returns a list of organization ids.

    add_group(group_id)
        Adds a group to the authorization model.

    get_groups()
        Returns a list of group ids.

    filter_dict(input, fields, whitelist)
        Filters a dictionary input to conform to the model fields if the key is in whitelist.

    Notes
    -----
    For information on the Organization, Groups and other traits, 
    see the `CKAN documentation <https://docs.ckan.org>`

    """

    def add_org(self, org_id):
        """ 
        Add an organization to the authorization model with org_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_org(self, org_id)" % (self.__class__.__name__))

    def get_orgs(self):
        """
        Returns a list of org_ids.
        """
        raise NotImplementedError("Class %s doesn't implement get_orgs(self)" % (self.__class__.__name__))

    def add_group(self, group_id):
        """
        Add a group to the authorization model with group_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_group(self, group_id)" % (self.__class__.__name__))

    def get_groups(self):
        """
        Returns a list of group ids from authorization model.
        """
        raise NotImplementedError("Class %s doesn't implement get_groups(self)" % (self.__class__.name))

    def add_user(self, user_id):
        """
        Add a user to the authorization model with the user_id.
        """

        raise NotImplementedError("Class %s doesn't implement add_user(self, user_id)" % (self.__class__.__name__))

    def get_users(self):
        """
        Get a list of user_id s based on the current authorization model.
        """

        raise NotImplementedError("Class %s doesn't implement get_users(self)" % (self.__class__.__name__))

    def add_dataset(self, dataset_id, fields, owner_id):
        """
        Add a dataset with the current id (dataset_id), fields and owner_id to the authorization model.
        """

        raise NotImplementedError("Class %s doesn't implement add_dataset(self, dataset_id, fields, owner_id)" % (self.__class__.__name__))

    def get_metadata_fields(self, dataset_id):
        """
        Return a dictionary of metadata fields based on the dataset_id
        """

        raise NotImplementedError("Class %s doesn't implement get_metadata_fields(self, dataset_id)" % (self.__class__.__name__))


    def get_visible_fields(self, dataset_id, user_id):
        """
        Return the subset of metadata field ids in this dataset for which the user has access.
        """
        
        raise NotImplementedError("Class %s doesn't implement get_visible_fields(self, dataset_id, user_id)")


    def set_visible_fields(self, dataset_id, user_id, whitelist):
        """
        Set the visible fields for the particular user_id.
        """

        raise NotImplementedError("Class %s doesn't implement set_visible_fields(self, dataset_id, user_id, whitelist)" % (self.__class__.__name__))

    def filter_dict(self, input, fields, whitelist):
        """
        Filter a dictionary, returning only white-listed keys.

        Parameters
        ----------
        input : dict
            The dictionary to filter.
        fields: dict
            Dictionary representing the fields and ids the dictionary should contain.
        whitelist: list of uuids
            The list of permitted field ids.

        Returns
        -------
        the original dictionary input with fields corresponding to whitelist.      
        """

        #log.info(fields)

        # Trivially check input type
        if type(input) != dict:
            raise TypeError("Only dicts can be filtered recursively! Attempted to filter " + str(type(input)))

        # Iterate through the dictionary entries

        # TODO: I would use a comprehension + helper function.
        #  i.e. {k: v for k, v in input.items if filterLogicFn(k, v)}
        #  The original dictionary will stay in tact and in memory though.
        for key,value in input.items():

            log.info("Checking authorization for %s", str(key))

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
