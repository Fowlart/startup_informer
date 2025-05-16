searchResult=$(az cognitiveservices account keys list --name fowlart-language-service --resource-group rg-fowlartChat)
key=$(echo "$searchResult" | jq -r '.key2')
echo "key is>$key"