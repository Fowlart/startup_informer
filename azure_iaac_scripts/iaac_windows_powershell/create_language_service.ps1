$curr_date = Get-Date -Format "dddd_MM_dd_yyyy"
az deployment group create --name $curr_date `
    --resource-group "rg-fowlartChat" `
    --template-file "./language_service.json"