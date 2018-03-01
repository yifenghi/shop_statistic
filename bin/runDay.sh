#!/bin/sh 

set -ux
#work_dir=$(readlink -f $(dirname $0))
work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf

if [ $# -eq 1 ]
then
  date=$1
else
  exit 1
fi

log_date=`date -d "$date" +%Y%m%d`
machine_log_path="$work_dir/log/machine.$log_date.log"
employee_log_path="$work_dir/log/employee.$log_date.log"
customer_log_path="$work_dir/log/customer.$log_date.log"
clean_log_path="$work_dir/log/clean.$log_date.log"

sh $work_dir/bin/machineMessage.sh "$date" 2>&1 | tee -a "$machine_log_path"
sh $work_dir/bin/machineFilter.sh "$date" 2>&1 | tee -a "$machine_log_path"
sh $work_dir/bin/employeeMessage.sh "$date" 2>&1 | tee -a "$employee_log_path"
sh $work_dir/bin/visitCustomer.sh "$date" 2>&1 | tee -a "$customer_log_path"

sh $work_dir/bin/clean.sh "$date" 2>&1 | tee -a "$clean_log_path" 
