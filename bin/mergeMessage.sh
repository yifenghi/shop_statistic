#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class="com.doodod.market.statistic.MergeLauncher"
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml
if [ $# -eq 2 ]
then
  date=$1
  time_tag=$2
else
  echo "Usgae: $0 date time_tag"
  exit 1
fi

minute_tag=`date -d "$date" "+%M"`
hour_tag=`date -d "$date" "+%H%M"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
dir_name_old=`date -d "$date 5 minutes ago" "+%Y%m%d/%H/%M"`

input_part="$statistic/min/$dir_name/in*"
input_empty="$statistic/min/empty"
hexist $input_part
if [ $? != 0 ]
then
  input_part=$input_empty
fi
input_total="$statistic/$time_tag/$dir_name_old/part*"
if [ $time_tag == "hour" -a $minute_tag == $minute_empty_tag ]
then
  input_total=$input_empty
elif [ $time_tag == "day" -a $hour_tag == $hour_empty_tag ]
then
  input_total=$input_empty
fi

feature=$day_feature
model_path="$work_dir/data/model.$time_tag"
if [ $time_tag != "day" ]
then
  feature=$null_feature
  model_path="$work_dir/data/model.min"
fi

hexist $input_total
if [ $? == 1 ]
then
  input_total=$input_empty
fi

output="$statistic/$time_tag/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $model_path \
-libjars $jama \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D merge.input.part="$input_part" \
-D merge.input.total="$input_total" \
-D train.passenger.featurelist="$feature" \
-D train.model.path="`basename $model_path`" \
-D rssi.indoor.scope="$rssi_indoor"

exit 0;
