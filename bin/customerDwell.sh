#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.apply.CustomerLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
date_tail=`date -d "$date_now" "+%H:%M:%S"`
if [ $date_tail == "00:00:00" ]
then
  #when 00:00:00, update time should be set to yesterday.
  date_today=`date -d "$date_now 1 day ago" "+%Y%m%d"`
else
  date_today=`date -d "$date_now" "+%Y%m%d"`
fi
date_today=`date -d "$date_today" "+%Y-%m-%d %H:%M:%S"`
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`

##add output
input="$statistic/day/$dir_name"
output="$customer/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D customer.time.create="$date_now" \
-D customer.time.update="$date_today" \

exit 0;
