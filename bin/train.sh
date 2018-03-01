#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class="com.doodod.market.analyse.ClassifyTrainLauncher"
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml
if [ $# -eq 2 ]
then
  date_now=$1
  train_tag=$2
else
  echo "Usgae: $0 date time_tag"
  exit 1
fi

date_today=`date -d "$date_now" "+%Y%m%d"`
date_yesterday=`date -d "$date_now 1 day ago" "+%Y%m%d"`
dir_name=`date -d "$date_now" "+%Y%m%d/%H/%M"`


if [ $train_tag == "min" ]
then
  model="$work_dir/data/model.min"
  input="$statistic/$train_tag/$date_yesterday/1*/*/in*"
  feature=$minute_feature
elif [ $train_tag == "day" ]
then
  model="$work_dir/data/model.day"
  input="$statistic/$train_tag/$date_today/00/00/part*"
  feature=$day_feature
fi
model_today="$model.$date_today"

output="$statistic/model/$date_today/$train_tag"
hrmr $output 

jama="/var/lib/hadoop-hdfs/code/deploy/local_lib/Jama-1.0.3.jar"
HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-libjars $jama \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D train.passenger.featurelist="$feature" \
-D rssi.indoor.scope="$rssi_indoor"

htext "$output/part*" > $model_today
cp $model_today $model

exit 0;
