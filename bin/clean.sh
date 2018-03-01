#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

date_today=`date -d "$date" "+%Y%m%d"`
date_one_day=`date -d "$date 1 day ago" "+%Y%m%d"`
date_one_week=`date -d "$date 7 days ago" "+%Y%m%d"`
date_one_month=`date -d "$date 30 days ago" "+%Y%m%d"`

hrmr "$statistic/night/$date_one_week"
hrmr "$statistic/machine/$date_one_day"
hrmr "$statistic/employee/$date_one_day"

hrmr "$statistic/visit/$date_one_week"
hrmr "$statistic/visit/info/$date_one_day"
hrmr "$statistic/visit/customer/$date_one_day"

hrmr "$statistic/min/$date_one_day"
hrmr "$statistic/hour/$date_one_day"

hrmr "$statistic/day/$date_one_day/[12]*"
hrmr "$statistic/day/$date_one_day/0[!0]*"
hrmr "$statistic/day/$date_one_day/00/[!0]*"
hrmr "$statistic/day/$date_one_day/00/05"

hrmr "$statistic/day/$date_one_month"

hrmr "$flow/$date_one_day"
hrmr "$customer/$date_one_day"

rm "$work_dir/log/machine.$date_one_week.log"
rm "$work_dir/log/employee.$date_one_week.log"
rm "$work_dir/log/bi_store.$date_one_week.log"
rm "$work_dir/log/clean.$date_one_week.log"
