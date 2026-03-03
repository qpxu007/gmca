#!/bin/bash

# usage  
#  1. show_analysis.sh -- start new data viewer gui
#  2. show_analysis.sh analysis -- start analysis web ui
#  3. show_analysis.sh analysis -- start strategy web ui

#!/bin/bash

ANALYSIS_TITLE="Data Analysis"
STRATEGY_TITLE="Strategy"
DEFAULT_TITLE="GMCA Data Viewer"  

raise_or_open() {
  local mode="$1"
  local title="$2"
  local program_path="$3"
  echo "mode=$mode title=$title"

  # is the window open?
  if (( $(wmctrl -l|grep "$title"|wc -l) >= 1 ))
  then
    # raise the window
    wmctrl -a "$title"
  else
    # start the program
    $program_path
  fi
}

# Check if no arguments provided
if [[ $# -eq 0 ]]; then
  # Launch default program with raise_or_open
  raise_or_open "default" "$DEFAULT_TITLE" "/mnt/software/scripts/dv"
  exit 0
elif [[ $1 == "analysis" ]]; then
  mode="analysis"
  title=$ANALYSIS_TITLE
  program="python /mnt/software/pybluice/src/gui/analysis/browser.py $mode"
else
  mode="strategy"
  title=$STRATEGY_TITLE
  program="python /mnt/software/pybluice/src/gui/analysis/browser.py $mode"
fi

raise_or_open "$mode" "$title" "$program"
