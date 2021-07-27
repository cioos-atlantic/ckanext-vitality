# Dev Deployment Script

echo "Preventing calamity. Please wait while environment variables are verified..."
# ENSURE ENVIRONMENT VARIABLES EXIST, OTHERWISE CRASH                                                                                                                                        # IT'S CRITICAL THIS EXISTS OTHERWISE THE DEPLOYING SERVER WILL GET NUKED!!                                                                                                        
if [ -z "${CKAN_LOG_PATH}" ] ||\
   [ -z "${CKAN_PORT}" ] ||\
   [ -z "${CKAN_SITE_URL}" ] ||\
   [ -z "${CKAN_TAG}" ] ||\
   [ -z "${CKANEXTS_PATH}" ] ||\
   [ -z "${DOCKER_EXTS_PATH}" ] ||\
   [ -z "${DOCKER_VENV_PATH}" ] ||\
   [ -z "${DOCKER_PATH}" ] ||\
   [ -z "${EXT_NAME}" ]
then
    echo "Required environment variables not set! Exiting to prevent catastrophic behavior!"
    exit 1
fi

echo "CKAN_LOG_PATH: ${CKAN_LOG_PATH}"
echo "CKAN_PORT: ${CKAN_PORT}"
echo "CKAN_TAG: ${CKAN_TAG}"
echo "CKAN_SITE_URL: ${CKAN_SITE_URL}"
echo "CKANEXTS_PATH: ${CKANEXTS_PATH}"
echo "EXT_NAME: ${EXT_NAME}"
echo "DOCKER_PATH: ${DOCKER_PATH}"
echo "DOCKER_VENV_PATH: ${DOCKER_VENV_PATH}"
echo "DOCKER_EXTS_PATH: ${DOCKER_EXTS_PATH}"

# COPY PLUGIN FILES
echo "Cloned repo into ${AGENT_RELEASEDIRECTORY}/${RELEASE_ARTIFACTS__CIOOS-ATLANTIC_CKANEXT-VITALITY_PROTOTYPE_(1)_DEFINITIONNAME}/drop/"
echo "Repo contents:"
ls ${AGENT_RELEASEDIRECTORY}/${RELEASE_ARTIFACTS__CIOOS-ATLANTIC_CKANEXT-VITALITY_PROTOTYPE_(1)_DEFINITIONNAME}/drop/

# If the extension folder does not exist create it.
if [ ! -d ${CKANEXTS_PATH}${EXT_NAME} ]
then
    echo "Extension folder does not exist, creating it now."
    sudo mkdir ${CKANEXTS_PATH}${EXT_NAME}
else
    echo "Extension folder exists, clearing old contents in ${CKANEXTS_PATH}${EXT_NAME}"
    sudo rm -r ${CKANEXTS_PATH}${EXT_NAME}
    sudo mkdir ${CKANEXTS_PATH}${EXT_NAME}
fi
echo "Copying new files"
sudo cp -r ${AGENT_RELEASEDIRECTORY}/${RELEASE_PRIMARYARTIFACTSOURCEALIAS}/* ${CKANEXTS_PATH}${EXT_NAME}
sudo chmod 777 -R ${CKANEXTS_PATH}${EXT_NAME}

echo "Installing plugin into CKAN venv"
sudo docker exec ckan /bin/bash -c "source ${DOCKER_VENV_PATH} && \
    cd ${DOCKER_EXTS_PATH}${EXT_NAME}/ && pip install -r requirements.txt"
sudo docker exec ckan /bin/bash -c "source ${DOCKER_VENV_PATH} && \
    cd ${DOCKER_EXTS_PATH}${EXT_NAME}/ && python setup.py develop"

# SHUTDOWN CKAN
cd ${DOCKER_PATH}
sudo docker-compose down 

# RESTART CKAN
cd ${DOCKER_PATH}
sudo docker-compose up -d 

# SEED VITALITY AUTHORIZATION MODEL
echo "Seeding users into metadata authorization model"
sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=${EXT_NAME} \
    vitality seed --config=/etc/ckan/production.ini

# RE-INDEX DATASETS
echo "Re-indexing datasets..."
sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=ckan search-index \
    rebuild --config=/etc/ckan/production.ini

sudo docker exec ckan /usr/local/bin/ckan-paster --plugin=ckanext-harvest harvester \
    reindex --config=/etc/ckan/production.ini