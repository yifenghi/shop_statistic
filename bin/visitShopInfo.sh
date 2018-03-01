#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.apply.VisitShopInfoLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date_now=$1
fi
dir_name=`date -d "$date_now" "+%Y%m%d"`
today=`date -d "$date_now 1 second ago" "+%Y-%m-%d 00:00:00"`

##add output
input="$statistic/visit/$dir_name"
output="$statistic/visit/info/$dir_name"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D doodod.system.bizdate="$today" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \

exit 0;
