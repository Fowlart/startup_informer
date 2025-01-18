#!/bin/bash

cd /home/artur/PycharmProjects/startup_informer || exit 1

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

# handle the case of first launch. Telegram authentication needed.
# todo: this one makes app not scalable...
if [[ -f ./init_session.session ]]
then
  echo "Skipping Telegram login..."
else
  echo "Activate run with Telegram login..."
  .venv/bin/python3.12 schedule_sender.py
  exit 0
fi


.venv/bin/python3.12 schedule_sender.py >> ./startup_info 2>&1  || exit 1 &

if [[ $1 == "-a" ]]
then
 .venv/bin/python3.12 full_history_traverser.py  >> ./startup_info 2>&1 || exit 1 &
fi
wait