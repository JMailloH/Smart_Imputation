# Smart Imputation. k Nearest Neighbor Imputation

[Shot description] 

## Cite this software as:

[Reference]
 
# How to use

## Pre-requiriments
The following software have to get installed:
- Scala. Version 2.11.6
- Spark. Version 2.1.1
- Maven. Version 3.3.3
- JVM. Java Virtual Machine. Version 1.8.0 because Scala run over it.

## Download and build with maven
- Download source code: It is host on GitHub. To get the sources and compile them we will need git instructions. The specifically command is:

		```git clone https://github.com/JMailloH/Smart_Imputation.git ```

- Build jar file: Once we have the sources, we need maven and we change to the root directory of the project. Thus, call the compile bash script:

		```./compile.sh ```

It generates the jar file under /target/ directory

- Notice that you can clean the project with the bash script:

		```./clean.sh ```

Another alternative is downloading or using via spark-package at [enlace al paquete](enlace al paquete)

## How to run & examples

You could find a bash script for the 2 developed methods: 

- To run the examples of the k Nearest Neighbors Imputation with the Local and Global version:

	```./run.sh ``` You can find the source code of the example [here](https://github.com/JMailloH/Smart_Imputation/blob/master/src/main/scala/run/runSmart_Imputation.scala).

## Output
For global imputation, the folder "./dataset/page-blocksMVs10-IMPT-k3-maps10-global-tra.data" contains at "data" the dataset imputed. "Report" folder contains the Imputation runtime.
