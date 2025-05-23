# Telegram Message Analyser

- Application able to parse the Telegram messages and store them in delta-table format for further processing with the usage of ML/NLP techniques

- This application also serves as a laboratory for studying SaaS solutions, such as MS Fabric, Azure AI search, and Azure Language Service for Natural Language Processing-related tasks

**How to install on Linux Ubuntu:**

1/ fetch the project through /git clone: 
git clone https://github.com/Fowlart/startup_informer.git

2/ cd startup_informer

3/ **sudo** ./setup_on_linux.sh

4/ carefully follow by white rabbit...

### Note:

* This script requires root privileges to access system logs.

**Linux command to include and study**
- `crontab -e`
- `last reboot` 
- `journalctl -u NetworkManager.service`
- `awk`
- `sed`
- `cat /proc/sys/kernel/threads-max`
- editing `/etc/profile`

**Python concepts to include and study:**
- Asyncio
- Virtual env
- Processing data with Spark/SparkMl

**Todo:**

- A~~dd working with bash to automate install application on a new Linux VM~~
- ~~Add code to parse all Telegram user channels for looking up, messages by simple pattern(as for the starting point). 
Use multithreading/multiprocessing/asyncio to parse multiple channels simultaneously.~~
- ~~Organize the possibility of starting the bash script with the flag to spin up separate processes, traversing all
chat history in telegram without a time limit, and exporting it to the file. 
Send the file for analysis. 
A separate process should be created in Python. 
Flag recognition should be organized at the bash script.~~
- ~~Use Spark on Linux to build delta tables from messages~~
- ~~Add data transformation for reach and informative table structure~~
- ~~Collect data in star table pattern database(users, messages)~~
- ~~Adjust Linux deployment~~

- Add possibility to develop on windows
  - create run configurations for each Python script
  - add instructions for running on windows
  - ~~adjust https://sparknlp.org/ libs version for Windows 11~~

- Use Classification to classify messages
- explore using of https://sparknlp.org/

- Implement the tf/idf algorithm to find keywords in each message within the concrete channel
  - automate the creation of Azure resources needed
  - ~~explore using of https://sparknlp.org/~~

- Index all messages from telegram to Azure AI search, implement search by key-words from telegram

- Add Spark reading and writing analyzed data to the Azure blob storage (will reduce local paths resolution need, and will allow Spark local script execution)   

- Add single-time key extraction for Azure services(Keyring)

**Create workloads in MS Fabric:**

Docs:
1. [Data factory](https://learn.microsoft.com/en-us/fabric/data-factory/)
2. [Custom text classification](https://learn.microsoft.com/en-us/azure/ai-services/language-service/custom-text-classification/overview)

Tasks
1. [x] ~~Create a pipeline to ingest messages~~
2. [x] ~~Find a way to categorize messages into some predefined categories:~~
3. [x] ~~Create a Spark job in Fabric to create the labelled-data-file out of raw messages and a subset of labelled messages~~
4. [x] ~~Find a way to create and train a model in Azure AI Language from Fabric~~
5. [x] ~~Add a task in the pipeline to classify all messages using a trained model~~
6. [x] ~~Publish the result chart somehow~~ [report page](https://fowlartaisearchstore.z20.web.core.windows.net/)
7. [x] Automate deployment: Fabric pipline + Azure AI language Services.
_This part could not be achieved without git integration, which is accessible in the paid version only_.
8. [x] Add conditional logic to check if the number of labelled messages was changed, and run retraining of text classification based on it
    