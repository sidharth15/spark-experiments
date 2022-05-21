#!/bin/bash

# set the environment variables for spark and python before starting jupyter - if it's not already set
if [ -z "${SPARK_HOME}" ];
then
	echo "Setting environment variables for SPARK_HOME, PATH and PYTHONPATH"
	export SPARK_HOME=/home/ec2-user/spark-3.2.1-bin-hadoop3.2
	export PATH=$SPARK_HOME/bin:$PATH
	export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
fi

# start the history server - the script will take of handling if the server is already up and running
echo "Starting history server..."
$SPARK_HOME/sbin/start-history-server.sh

# this is needed when running jupyter on an ec2 instance, so that Jupyter listens on the specific public dns 
PUBLIC_DNS=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)
jupyter notebook --ip=$PUBLIC_DNS
