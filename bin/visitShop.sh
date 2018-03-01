#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.apply.VisitShopLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
today=`date -d "$date_now 1 second ago" "+%Y-%m-%d 00:00:00"`
hour_tag=`date -d "$date_now" "+%H:%M"`

dir_hour=`date -d "$date_now" "+%Y%m%d/%H/00"`
dir_today=`date -d "$date_now" "+%Y%m%d"`
dir_name_yesterday=`date -d "$date_now 1 day ago" "+%Y%m%d"`

if [ $hour_tag == "00:00" ]
then
    echo "haha"
fi

##add output
input_empty="$statistic/min/empty"
input_day="$statistic/day/$dir_hour"
input_merge="$statistic/visit/$dir_name_yesterday"
hexist $input_merge
if [ $? == 1 ]
then
  input_merge=$input_empty
fi

output="$statistic/visit/$dir_today"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D doodod.system.bizdate="$today" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D merge.input.part="$input_day" \
-D merge.input.total="$input_merge" \

if [ $hour_tag == "00:00" ]
then
   hrmr "$statistic/visit/$dir_name_yesterday/*"
   hcp "$output/*" "$statistic/visit/$dir_name_yesterday" 
fi

exit 0;
