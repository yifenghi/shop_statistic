#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.apply.FlowLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`

##add output
input_empty="$statistic/min/empty"
input_min="$statistic/min/$dir_name/in*"
input_hour="$statistic/hour/$dir_name/part*"
input_day="$statistic/day/$dir_name/part*"
hexist $input_min
if [ $? == 1 ]
then
  input_min=$input_empty
fi

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
-D flow.input.min="$input_min" \
-D flow.input.hour="$input_hour" \
-D flow.input.day="$input_day" \

exit 0;
