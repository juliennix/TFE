TFE (in construction)
Learning Graphical Probabilistic Models
=======================================
### How to Run
Use sbt to package the code, type in the shell :
$ sbt
> package
When the package is done, you are ready to launch and use it

In order to run it in the spark shell, type in the shell : 

/spark-1.2.1/bin/spark-shell --master local[number_of_local_core] --jars spark_code_2.10-1.0.jar,jfreechart-1.0.13.jar,jcommon-1.0.23.jar,jung-algorithms-2.0.1.jar,jung-visualization-2.0.1.jar,collections-generic-4.01.jar,jung-api-2.0.1.jar,jung-graph-impl-2.0.1.jar 

In order to use the main file (should be re-write by yourself), type in the shell : 

/spark-1.2.1/bin/spark-submit --class "Main" spark_code_2.10-1.0.jar --jars jfreechart-1.0.13.jar,jcommon-1.0.23.jar,jung-algorithms-2.0.1.jar,jung-visualization-2.0.1.jar,collections-generic-4.01.jar,jung-api-2.0.1.jar,jung-graph-impl-2.0.1.jar 

Feel free to tune the spark configurations

Dependencies
------------
### Spark and GraphX
Main dependencies
### Jung2
Visualization tools for graph and tree
### JFreeChart
Visualization tools for graphics

1. Overview
===========
Chow liu Algortihm

### Mutual information
In order to find probability dependencies between variables, the mutual information is commonly used.
Build on the shannon's entropy and the conditional one, a matrix of mutual information is used to construct the maximum spanning tree.
### Maximum Weight Spanning Tree
### Markov Tree Generation
Representation of the tree :
### Learning Parameters
### Inference
Representation of the tree : 
### Mixture
