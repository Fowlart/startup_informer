# Telegram Linux Startup informer

- This script parses system logs on startup to identify the last boot timestamp and the wifi connection used during the previous session. 

- All logs are sending to the Telegram.


**How to Use:**
1. Make the script executable: `chmod +x tg_startup_informer.sh`
2. Add the script to your system startup process. Refer to your system's documentation 
for specific instructions on how to do this (e.g., in my case, editing `/etc/profile` to start the script on user login 
and set up environment variables).

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
- Threading vs Multiprocessing
- Asyncio
- Virtual env
- Working with dates[standard library]

**Todo:**

- Add working with bash to automate install application on a new Linux VM

- Add code to parse all Telegram user channels for looking up, messages by simple pattern(as for the starting point). 
Use multithreading/multiprocessing/asyncio to parse multiple channels simultaneously. 

- ~~Organize the possibility of starting the bash script with the flag to spin up separate processes, traversing all
chat history in telegram without a time limit, and exporting it to the file. 
Send the file for analysis. 
A separate process should be created in Python. 
Flag recognition should be organized at the bash script.~~


**Ideas for another projects:**

- Index all messages from telegram to Azure AI search, implement search be key-words from telegram
- Use Spark ML on Linux, to analyze through messages
- Create a daemon which will track access to folders of interest in Linux, and will send notifications about