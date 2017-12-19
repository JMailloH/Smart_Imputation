#!/bin/bash

/opt/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --master local[*] --class org.apache.spark.run.runSmart_Imputation ./target/Smart_Imputation-1.0.jar ./dataset/page-blocks.header ./dataset/page-blocksMVs10tra.data 3 10 local ./dataset/page-blocksMVs10-IMPT-k3-maps10-local-tra.data

/opt/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --master local[*] --class org.apache.spark.run.runSmart_Imputation ./target/Smart_Imputation-1.0.jar ./dataset/page-blocks.header ./dataset/page-blocksMVs10tra.data 3 10 global ./dataset/page-blocksMVs10-IMPT-k3-maps10-global-tra.data

