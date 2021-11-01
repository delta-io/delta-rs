#!/bin/bash

export AZURE_STORAGE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;'

function wait_for() {
  retries=10
  until eval $2 > /dev/null 2>&1
  do
    if [ "$retries" -lt "0" ]; then
      echo "$1 is still offline after 10 retries";
      exit 1;
    fi
    echo "Waiting on $1 to start..."
    sleep 5
    retries=$((retries - 1))
  done
}

# We need to use azcopy natively and not via az, since the local url is not recognized autmoatically by
# azcopy as a blob localtion and we cannot pass the '--from-to' flag down into the az commands. 
wget -O azcopy_v10.tar.gz https://aka.ms/downloadazcopy-v10-linux && tar -xf azcopy_v10.tar.gz --strip-components=1
cp ./azcopy /usr/bin/

az config set extension.use_dynamic_install=yes_without_prompt

# azcopy does not support authentication via account key, thus we generate an saas key
SAS=$(az storage account generate-sas --permissions cdlruwap --services b --resource-types sco --expiry 2030-01-01 -o tsv)

# create account and copy data
az storage fs create -n deltars
azcopy copy "data/golden" "http://azurite:10000/devstoreaccount1/deltars/?$SAS" --recursive=true --from-to=LocalBlob
azcopy copy "data/simple_table" "http://azurite:10000/devstoreaccount1/deltars/?$SAS" --recursive=true --from-to=LocalBlob