#!/bin/bash

/opt/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master local[*] --class org.apache.spark.run.runkNNI_IS ~/Dropbox/Workspaces/kNNI_IS/target/kNNI_IS-2.0.jar ~/datasets/page-blocks-5-fold/page-blocks.header ~/datasets/page-blocks-5-fold/page-blocksMVs10-5-1tra.data 1 10 5 -1 ~/datasets/page-blocks-5-fold/page-blocksMVs-IMPUTED-10-5-1tra.data 5 exact-model

#nohup /opt/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master local[*] --class org.apache.spark.run.runkNNI_IS ~/Dropbox/Workspaces/kNNI_IS/target/kNNI_IS-2.0.jar ~/datasets/page-blocks-5-fold/page-blocks.header ~/datasets/page-blocks-5-fold/page-blocksMVs10-5-1tra.data 1 5 5 -1 ~/datasets/page-blocks-5-fold/page-blocksMVs-IMPUTED-5-5-1tra.data 5 exact-model&


#/opt/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master local[*] --class org.apache.spark.run.runkNNI_IS ~/Dropbox/Workspaces/kNNI_IS/target/kNNI_IS-2.0.jar ~/datasets/page-blocks-5-fold/page-blocks.header ~/datasets/page-blocks-5-fold/page-blocksMVs10-5-1tra.data 3 10 5 -1 ~/datasets/page-blocks-5-fold/page-blocksMVs-IMPUTED-10-5-3tra.data 5 exact-model

#/opt/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --master local[*] --class org.apache.spark.run.runkNNI_IS ~/Dropbox/Workspaces/kNNI_IS/target/kNNI_IS-2.0.jar ~/datasets/page-blocks-5-fold/page-blocks.header ~/datasets/page-blocks-5-fold/page-blocksMVs10-5-1tra.data 3 5 5 -1 ~/datasets/page-blocks-5-fold/page-blocksMVs-IMPUTED-5-5-3tra.data 5 exact-model


