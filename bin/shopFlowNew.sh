#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.apply.FlowLauncherNew
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`
date_5min_ago=`date -d "$date_now 5 mins ago" "+%Y-%m-%d %H:%M:%S"`
date_current_hour=`date -d "$date_now" "+%Y-%m-%d %H:00:00"`

##add output
input="$statistic/day/$dir_name"
#hexist $input_min
#if [ $? == 1 ]
#then
#  input_min=$input_empty
#fi

output="$flow/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mall.info.time.stamp="$date_now" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D time.tag.min="$date_5min_ago" \
-D time.tag.hour="$date_current_hour"

exit 0;
