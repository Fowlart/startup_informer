#!/bin/bash

# Execute the Azure CLI command and capture the output
searchResult=$(az search admin-key show --service-name fowlart-ai-search --resource-group rg-fowlartChat --output json)
key=$(echo "$searchResult" | jq -r '.primaryKey')

# Print the key
echo "key is>$key"