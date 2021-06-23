# Dev Deployment Script

# ENSURE ENVIRONMENT VARIABLES EXIST, OTHERWISE CRASH
# IT'S CRITICAL THIS EXISTS OTHERWISE THE DEPLOYING SERVER WILL GET NUKED!!
if [ -z "$(ckan_log_path)"] ||
   [ -z "$(ckan_port)"] ||
   [ -z "$(ckan_site_url)"] ||
   [ -z "$(ckan_tag)"] ||
   [ -z "$(ckanexts_path)"] ||
   [ -z "$(docker_exts_path)"] ||
   [ -z "$(docker_venv_path)"] ||
   [ -z "$(docker_path)"] ||
   [ -z "$(ext_name)"] 
then
    echo "Required environment variables not set! Exiting to prevent catastrophic behavior!"
    exit 1

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