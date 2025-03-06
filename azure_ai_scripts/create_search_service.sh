#!/bin/bash
# sku free - no symantic search(use sku basic to explore it)
az search service create \
--name fowlart-ai-search \
--resource-group rg-fowlartChat \
--sku free