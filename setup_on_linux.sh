#!/bin/bash
# 3. Update and upgrade package lists (assuming root privileges are not required)
sudo apt update && sudo apt upgrade -y  # -y for unattended yes to prompts

# 4. Install python3.12-venv, java (check if already installed)
if ! command -v python3.12-venv &> /dev/null
then
  sudo apt install python3.12-venv
fi

if ! command -v java -version &> /dev/null
then
  sudo sudo apt install openjdk-11-jre-headless
fi

# 5. Create virtual environment
python3 -m venv ./.venv

# 6. Activate virtual environment (source or alternative using venv module)
source ./.venv/bin/activate  # Or: . ./.venv/bin/activate

# 7. Install dependencies from requirements.txt
pip install -r requirements.txt

echo "Please enter TELEGRAM_API_ID: "
read TELEGRAM_API_ID
export TELEGRAM_API_ID=$TELEGRAM_API_ID
sudo echo "export TELEGRAM_API_ID=$TELEGRAM_API_ID" | sudo tee -a /etc/profile
echo "Please enter TELEGRAM_API_HASH: "
read TELEGRAM_API_HASH
export TELEGRAM_API_HASH=$TELEGRAM_API_HASH
sudo echo "export TELEGRAM_API_HASH=$TELEGRAM_API_HASH" | sudo tee -a /etc/profile
sudo echo "$(pwd)/tg_startup_informer.sh" | sudo tee -a /etc/profile


# 8. Telegram authentication (assuming a script exists)
# Since this step involves user interaction, it's left for manual execution.
echo "Telegram authentication required for first launch. Please run the script manually."
./tg_startup_informer.sh
# 9. Deactivate virtual environment (optional)
deactivate

echo "Linux VM setup and Python application preparation completed!"