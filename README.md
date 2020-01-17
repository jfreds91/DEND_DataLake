# Data Engineering Nanodegree
# Project: AWS DataLake

Jesse Fredrickson

1/17/20

## Purpose
The purpose of this project is to set up an ETL (Extract, Transform, Load) pipeline to transport data from an Amazon S3 bucket into fact and dimension tables stored on an the same S3 cluster. The source data is quite large, and I will be using an AWS EMR cluster to run a spark job to process the data.

## Method
The S3 bucket, owned by Udacity, contains all of the source json files that are to be processed. I set up an EMR cluster, ssh into it, and run my code (etl.py) which deploys a spark job to the Master node of the EMR cluster.

## Files
- **etl<i></i>.py:** Contains main logic for ETL pipeline to populate SQL tables from json files, and save them back to S3. Udacity S3 bucket info is hardcoded.
- **secret.cfg:** (NOT INCLUDED) contains configuration info for user KEY and SECRET

## Usage
1. Ensure that the 'secret' configuration file exists and is populated correctly.
2. Create an AWS EMR cluster with at least 1 master node, and 1 slave node. You will need to generate an EC2 key pair to associate with this EMR cluster (can be done using links from the EMR creation page and PuTTY). The node must have Spark installed.
3. Edit the Security Group of the EMR Master node for inbound traffic to allow SSH connections via port 22.
On the Master node main AWS page, click SSH next to the Master Public DNS, and copy the DNS link (hadoop<i></i>@ec2-55-555-555-555.us...).
4. Open PuTTY, select Session, and paste that DNS into the destination field.
5. In putty, expand SSH and click Auth, and browse for the EC2 key pair that's associated with the master node.
6. Initiate the connection and click 'yes' on the security warning.
7. Change the default python environment on the master node from python 2.x to python 3.x: enter the command

`sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh`

8. Verify that python 3 is the default for the pyspark environment by typing `pyspark`
9. Recreate the python file and configuration file here... may be done with a tunnel, or by just using nano to create the files from scratch (can copy-paste code).
10. Submit the spark job with:

`/usr/bin/spark-submit --master yarn ./etl.py`


## Schema
songplays is the fact table, the rest are all dimension tables

![](schema.png)
