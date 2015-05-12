TFE (in construction)
Learning Graphical Probabilistic Models
=======================================
### What for
This library is 

### How to Run
Use sbt to package the code, type in the shell :

$ sbt

\> package

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
### Content File
The content file contains the variables samples, each line corresponds the label of the variable separated by a coma and then a set of observations of one variable separated by a blank space.
e.g. = 
1, 0 1 0 0 0 1 0 0 0 1 1 1 0
2, 0 1 0 0 1 1 1 1 0 0 0 1 0
...
Different part of the Chow-Liu algorithm to construct trees
### Mutual information
In order to find probability dependencies between variables, the mutual information is commonly used.
Build on the shannon's entropy and the conditional one, a matrix of mutual information is used to construct the maximum spanning tree.
### Contruct a graph from the mutual information
Use the RDDFastGraph() function to create a graph that will directly be computed thanks to the mutual information between the provided variable in the content file.
Use the GHSGraph() function to create a graph that should be use to compute the maximum spanning tree through the GHS function which is a message passing algorithm.
### Maximum Weight Spanning Tree
Different methods to construct the MWST are given
With the graph created from the function RDDFastGraph()

1. Functions that run only on local drive
kruskalEdgesAndVertices
PrimsAlgo
boruvkaAlgo
2. Functions that run partially on RDD and on local drive (see the specification)
kruskalEdges
kruskalEdgeRDD

PrimsDistFuncEdge
PrimsEdge

3. Functions that run only on RDD
(be warned that using those functions on your own laptop may be very computationally expensive)
PrimsRDD
boruvkaDistAlgo

With the graph created from the function GHSGraph()
(This function use a lot a RAM but is less heavy than the others on RDD, use this function
GHSMwst

### Markov Tree Generation
Representation of the tree :
The markov tree in composed of nodes and edges
The nodes are defined this way :
The edges are defined this way :


Function to create trees : 
markovTreeCreation(MWST)
### Learning Parameters
In order to learn parameters from a markov tree use the function learnParameters(markovTree, content)
The estimation of the parameters are base on the maximum likelijood estimation
### Inference
The inference is done by the belief propagation algorithm

In order to perform inference, the structure of the given tree will be changed in order to keep into that tree the belief, the lambda and pi messages, the conditional probability table... The representation of the tree is divided in two parts : the firt phase where lambda messages are send from the leaves to the root by level and the second phase where pi messages are send from the root to the leaves and computing the belief.

Tree structure for the first phase : 

Tree structure for the second pahse :
### Mixture
MCL-B algorithm (to reduce the variance)

In order to create mixture, the former methods can be used. The mixture is created by boostrapping (resample without replacement) the content of the provided files. Then for each bootstrap sample, a tree is created. You may specified the size of the mixture. (to change the methods, replace in the mixture.scala file the methods used by the one you want to test, be warned that certain methods can be very computationally expensive)
 
EM-MT algorithm (to reduce the bias)
 

 
 
 
 
 
 
 







