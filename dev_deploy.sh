# Dev Deployment Script

echo "Preventing calamity. Please wait while environment variables are verified..."
# ENSURE ENVIRONMENT VARIABLES EXIST, OTHERWISE CRASH
# IT'S CRITICAL THIS EXISTS OTHERWISE THE DEPLOYING SERVER WILL GET NUKED!!
if [ -z "$CKAN_LOG_PATH" ||
    -z "$CKAN_PORT" ||
    -z "$CKAN_SITE_URL" ||
    -z "$CKAN_TAG" ||
    -z "$CKANEXTS_PATH" ||
    -z "$DOCKER_EXTS_PATH" ||
    -z "$DOCKER_VENV_PATH" ||
    -z "$DOCKER_PATH" ||
    -z "$EXT_NAME"] 
then
    echo "Required environment variables not set! Exiting to prevent catastrophic behavior!"
    exit 1
fi

echo $CKAN_LOG_PATH
echo $CKAN_PORT
echo $CKAN_TAG
echo $CKAN_SITE_URL
echo $CKANEXTS_PATH
echo $EXT_NAME
echo $DOCKER_PATH
echo $DOCKER_VENV_PATH

# # COPY PLUGIN FILES
# echo "Cloned repo into $(Pipeline.Workspace)/s/"
# echo "Repo contents:"
# ls $(Pipeline.Workspace)/s/

# # If the extension folder does not exist create it.
# if [ ! -d $(CKANEXTS_PATH)$(EXT_NAME) ]
# then
#     echo "Extension folder does not exist, creating it now."
#     sudo mkdir $(ckanexts_path)$(ext_name)
# else
#     echo "Extension folder exists, clearing old contents!"
#     sudo rm -r $(ckanexts_path)$(ext_name)/*
# fi
# echo "Copying new files"
# sudo cp -r $(Pipeline.Workspace)/s/* $(ckanexts_path)$(ext_name)
# sudo chmod 777 -R $(ckanexts_path)$(ext_name)

# echo "Installing plugin into CKAN venv"
# sudo docker exec ckan /bin/bash -c "source $(docker_venv_path) && \
#     cd $(docker_exts_path)ckanext-vitality_prototype/ && pip install -r requirements.txt"
# sudo docker exec ckan /bin/bash -c "source $(docker_venv_path) && \
#     cd $(docker_exts_path)ckanext-vitality_prototype/ && python setup.py develop"

# # SHUTDOWN CKAN
# cd $(docker_path)
# sudo docker-compose down 

# # RESTART CKAN
# cd $(docker_path)
# sudo docker-compose up -d 

# # SEED VITALITY AUTHORIZATION MODEL
# echo "Seeding users into metadata authorization model"
# sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=ckanext-vitality_prototype \
#     vitality seed --config=/etc/ckan/production.ini

# # RE-INDEX DATASETS
# echo "Re-indexing datasets..."
# sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=ckan search-index \
#     rebuild --config=/etc/ckan/production.ini

# sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=ckanext-harvest harvester \
#     reindex --config=/etc/ckan/production.ini