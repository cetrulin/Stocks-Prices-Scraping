#!/bin/bash 

# Defining paths
script_path='/home/YOUR_USER/PYTHON_SCRIPT_PATH/' # ending in '/'
script_name='retrieve_intraday_minutes.py'
log_path='/home/YOUR_USER/OUTPUT_LOGS_PATH/' # ending in '/'

# Current date for log name
now=$(date +'%d-%m-%Y') 

echo "sudo python $script_path$script_name > '$log_path'retrieve_intraday_minutes_'$now'.log'" > $script_path'output.txt'

# Running script
sudo python $script_path$script_name >$log_path'retrieve_intraday_minutes_'$now'.log'