#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Working directory $SCRIPT_DIR"

cd $SCRIPT_DIR

function get_restart_and_connection_info()
  {
    prev_day=$(date -d '-1 day' +'%a')
    p_prev_day=$(date -d '-2 day' +'%a')
    w_prev_day=$(date -d '-1 day' +'%b %d')
    p_w_prev_day=$(date -d '-2 day' +'%b %d')
    echo "Current date: $(date)";
    printf "Pattern to search in logs: %s | %s | %s | %s" "$prev_day" "$p_prev_day" "$w_prev_day" "$p_w_prev_day";
    echo ""
    echo "Printing info about recent restarts:"
    last reboot | head -n 10 | grep -E "^.*$prev_day|$p_prev_day.*$" ;
    echo "Printing info about wi-fi:";
    output=$(journalctl -u NetworkManager.service | tail -n 5000 | grep -E "^$w_prev_day.*Connected to wireless network.*$|^$p_w_prev_day.*Connected to wireless network.*$");
    echo "$output"
    the_list_uniq_networks=$(echo "$output" | awk '{ for (i=NF; i>0; i--) printf("%s ",$i); printf("\n")}' | sed 's/ .*//' | uniq);
    printf  "Connected wireless networks, in a recent 2 days\n"
    printf "%s" "$the_list_uniq_networks"
  }

if [[ $TELEGRAM_API_ID != "" &&  $TELEGRAM_API_HASH != "" ]]
then
	echo "Environment variables were set."
else
  echo "For the application to correct run, the environment variables below should be provided:"
  for x in "TELEGRAM_API_ID" "TELEGRAM_API_HASH"
    do
    echo " - $x"
    done
    exit 1
fi

get_restart_and_connection_info > ./startup_info

chmod a+rwx ./startup_info

# handle the case of first launch. Telegram authentication needed.
# todo: this one makes app not scalable...
if [[ -f ./init_session.session ]]
then
  echo "Skipping Telegram login..."
else
  echo "Activate run with Telegram login..."
  .venv/bin/python3.12 ./execution_scenarios/schedule_sender.py
  sudo chmod a+rwx ./init_session.session
  exit 0
fi

.venv/bin/python3.12  ./execution_scenarios/schedule_sender.py