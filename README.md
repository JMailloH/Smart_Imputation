# Smart Imputation. k Nearest Neighbor Imputation

The massive data collection may contain errors, being one of the most common errors known as the Missing Values problem (MVs). The MVs problem is due to the fact that there are blank gaps in the attribute variables of the training data. These MVs are often caused by human or sensor errors during data collection. This will influence the quality of the data and therefore alter the behavior of data mining algorithms negatively.

We present an open-source Spark package about an imputation algorithm based on k-nearest neighbors. We take advantage of the Apache Spark environment, how its in-memory operations to simultaneously impute big amounts of missing values for a big dataset. Two approaches have been designed and developed: local and global. The local approach is called k-nearest neighbors Local Imputation (kNN-LI). kNN-LI applies a divide and conquers approach using the MapReduce paradigm. Specifically, the data is divided among the available compute nodes, and the kNN imputer [1] is applied for each of the partitions. With this direct approach, it is intended to have a first quick approximation that allows us to work with the missing values. The global approach is called k-nearest neighbors Global Imputation (kNN-GI). kNN-GI calculates the k-nearest neighbors in an exact and distributed way. To do this, it follows the workflow scheme of the kNN-IS algorithm [2]. This is a slower approach than kNN-LI. It requires a more powerful cluster of nodes for good performance. Specifically, kNN-GI separates instances that contain MVs from those that do not (not-MVs dataset). For each instance that have MVs, its k-neighbors is calculated from the whole not-MVs dataset. Impute the blank gaps with the mean (or mode if the features are categorical) of the selected k neighbors. When the allocation has been made, the allocated instances are joined with the not-MVs dataset.

## References

[1] Batista, G. E. A. P. A. & Monard, M. C. (2003). An Analysis of Four Missing Data Treatment Methods for Supervised Learning. Applied Artificial Intelligence, 17, 519-533.

[2] Maillo, J., Ramírez-Gallego, S., Triguero, I. & Herrera, F. (2017). kNN-IS: An Iterative Spark-based design of the k-Nearest Neighbors classifier for big data. Knowledge-Based System, 117, 3-15. DOI: [10.1016/j.knosys.2016.06.012](https://doi.org/10.1016/j.knosys.2016.06.012)

## Cite this software as:

I. Triguero, D. García-Gil, J. Maillo, J. Luengo, S. García, F. Herrera. Transforming big data into smart data: An insight on the use of the k nearest neighbors algorithm to obtain quality data. Wiley Interdisciplinary Reviews. Data Mining and Knowledge Discovery. e1289. doi: 10.1002/widm.1289

# How to use

## Pre-requiriments
The following software have to get installed:
- Scala. Version 2.11.6
- Spark. Version 2.1.1
- Maven. Version 3.3.3
- JVM. Java Virtual Machine. Version 1.8.0 because Scala run over it.

## Download and build with maven
- Download source code: It is host on GitHub. To get the sources and compile them we will need git instructions. The specifically command is:

		git clone https://github.com/JMailloH/Smart_Imputation.git

- Build jar file: Once we have the sources, we need maven and we change to the root directory of the project. Thus, call the compile bash script:

		./compile.sh

It generates the jar file under /target/ directory

- Notice that you can clean the project with the bash script:

		./clean.sh

Another alternative is downloading or using via spark-package at [enlace al paquete](enlace al paquete)

## How to run & examples

You could find a bash script for the 2 developed methods: 

- To run the examples of the k Nearest Neighbors Imputation with the Local and Global version:

	./run.sh

You can find the source code of the example [here](https://github.com/JMailloH/Smart_Imputation/blob/master/src/main/scala/run/runSmart_Imputation.scala).

## Output
For global imputation, the folder "./dataset/page-blocksMVs10-IMPT-k3-maps10-global-tra.data" contains at "data" the dataset imputed. "Report" folder contains the Imputation runtime.
