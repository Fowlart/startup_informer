# Telegram Analyser

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
  ~~- explore using of https://sparknlp.org/~~

- Index all messages from telegram to Azure AI search, implement search by key-words from telegram

- Add Spark reading and writing analyzed data to the Azure blob storage (will reduce local paths resolution need, and will allow Spark local script execution)   

- Add single-time key extraction for Azure services(Keyring)

**Moving workloads to MS Fabric:**

- Add the script and the PyCharm Windows configuration to ingest data from TG into Azure ADLS Gen 2 directly as a Delta table
