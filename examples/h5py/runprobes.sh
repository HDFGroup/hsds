#!/bin/bash

# The number of threads (this must be a multiple of the # of testing routines, scripts corrects automatically)
THRDS=4

# The number of iterations (each testing routine will be run ITER times)
ITER=2

# The expected variable name within the testing files/data sources
VAR=tasmax

# Results output bucket (S3). Note: aws cli credentials with S3 write access are needed for this
ITYPE=`curl http://169.254.169.254/latest/meta-data/instance-type 2> /dev/null`
RESBUCK=hsds/tests/$ITYPE

#---------------------------------------------------------------------------
function filebased() {
   DT=`date +"%Y%m%d%H%M%S"`
   TDIR=/mnt/data
   mkdir $TDIR
   curdir=`pwd`
   cd $TDIR && { time xargs -n 1 curl -L -O < $curdir/urls.tst.txt 2> $curdir/download.$DT.txt ; cd -; }
   fls=(`find $TDIR/ -name "*.nc"`) 
   for f in ${fls[@]}; do
      fout=`basename $f`
      ./probeclimh5.py $f $VAR $THRDS $ITER > $fout.tst.$DT.csv 
      aws s3 cp $fout.tst.$DT.csv s3://$RESBUCK/$fout.tst.$DT.csv 
      aws s3 cp download.$DT.txt s3://$RESBUCK/download.$DT.txt 
   done
}

#---------------------------------------------------------------------------
function restbased() {
   DT=`date +"%Y%m%d%H%M%S"`
   while read line;  do
      fout=`basename $line`
      ./probeclimh5.py $line $VAR $THRDS $ITER > $fout.tst.$DT.csv 
      aws s3 cp $fout.tst.$DT.csv s3://$RESBUCK/$fout.tst.$DT.csv 
   done < urls-h5srv.tst.txt
}

filebased
restbased

DT=`date +"%Y%m%d%H%M%S"`
aws s3 cp perftest.log s3://$RESBUCK/perftest.$DT.log
