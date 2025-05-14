#!/bin/bash

# Get the current date in the format "dddd_MM_dd_yyyy"
curr_date=$(date +"%A_%m_%d_%Y")

# Execute the Azure CLI command
az deployment group create \
    --name "$curr_date" \
    --resource-group "rg-fowlartChat" \
    --template-file "./language_service.json"