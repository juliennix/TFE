TFE (in construction)
Learning Graphical Probabilistic Models
=======================================
### What for
This library is a machine learning library that use probabilistic graphical models to represent distribution.
This library creates mixtures of Markov trees and inference can be performed on the models. There is two algortihms that create mixtures: the MCL-B (Mixtures Chow-Liu with Bootstrap) that reduces the variance and the EM-MT (Expectation Maximization - Mixture Tree) that reduces the bias.

### How to Run
Use sbt to package the code, type in the shell :

$ sbt

\> package

When the package is done, you are ready to launch and use it

In order to run it in the spark shell, type in the shell : 

/spark-1.2.1/bin/spark-shell --master local[number_of_local_core] --jars spark-code_2.10-1.0.jar 

In order to use the main file (should be re-write by yourself), type in the shell : 

/spark-1.2.1/bin/spark-submit --class "Main" spark-code_2.10-1.0.jar 

Feel free to tune the spark configurations

Dependencies
------------
### Spark and GraphX
Main dependencies
### Jung2
(need to add the .jar: jung-algorithms-2.0.1.jar,jung-visualization-2.0.1.jar,collections-generic-4.01.jar,jung-api-2.0.1.jar,jung-graph-impl-2.0.1.jar, uploaded in the directory dependencies_jars)

Visualization tools for graph and tree
### JFreeChart  (need to add the .jar : jfreechart-1.0.13.jar,jcommon-1.0.23.jar, uploaded in the directory dependencies_jars) 
Visualization tools for graphics

1. Overview
===========
### Content File
The content file contains the variables samples, each line corresponds the label of the variable separated by a coma and then a set of observations of one variable separated by a blank space.

e.g. :

1, 0 1 0 0 0 1 0 0 0 1 1 1 0

2, 0 1 0 0 1 1 1 1 0 0 0 1 0

...

Different part of the Chow-Liu algorithm to construct trees
-----------------------------------------------------------
### Mutual information
In order to find probability dependencies between variables, the mutual information is commonly used.
Build on the shannon's entropy and the conditional one, a matrix of mutual information is used to construct the maximum spanning tree.
### Contruct a graph from the mutual information
Use the RDDFastGraph() function to create a graph that will directly be computed thanks to the mutual information between the provided variable in the content file.
Use the GHSGraph() function to create a graph that should be use to compute the maximum spanning tree through the GHS function which is a message passing algorithm.
### Maximum Weight Spanning Tree
Different methods to construct the MWST are given
With the graph created from the function RDDFastGraph()

* Functions that run only on local drive

 1. kruskalEdgesAndVertices
 2. PrimsAlgo
 3. boruvkaAlgo

* Functions that run partially on RDD and on local drive (see the specification)

 1. kruskalEdges
 2. kruskalEdgeRDD
 3. PrimsDistFuncEdge
 4. PrimsEdge

* Functions that run only on RDD

(be warned that using those functions on your own laptop may be very computationally expensive)

 1. PrimsRDD
 2. boruvkaDistAlgo

* Message passing algorithm
With the graph created from the function GHSGraph()

(This function use a lot a RAM but is less heavy than the others on RDD, use this function)

 1. GHSMwst

As the structure of the GHSGraph is a little bit more complex, here is its representation: 
The nodes are defined this way:
GHSNode.

* Fragment 

Fragment.

* id ; VertexId : it corresponds to the 
* lastFragmentId ; VertexId : it corresponds to the 
* minWeight ; Double : it corresponds to the minimum weight that will be used to choose the minimum weight in a fragment

* AddedLink

### Markov Tree Generation
Representation of the tree :
The markov tree in composed of nodes and edges
The nodes are defined this way :
MarkoNode.

* level ; Double : the root has the level 0 then the next level of variables have 1 and so on.
* cpt ; Map[JointEvent, Probability] : the conditional probability table

cpt.

* JointEvent : It represents the joint values that can be taken by the variable and than the parent of this variable.

JoinEvent.

* Variable ; Double : the value of an occurrence of the variable
* Condition ; Double : the value of an occurrence of the condition

Probability : the corresponding probability of the JointEvent

Probability.

* value ; Double : the probability of the joint event

The edges are simply define as Double. This Double represents the weight of the edge i.e. the mutual information between the two variable it links.

Use the function markovTreeCreation(MWST) to create Markov trees.

### Learning Parameters
In order to learn parameters from a markov tree use the function learnParameters(markovTree, content).
The estimation of the parameters are base on the maximum likelijood estimation

### Inference
The inference is done by the belief propagation algorithm

In order to perform inference, the structure of the given tree will be changed in order to keep into that tree the belief, the lambda and pi messages, the conditional probability table... The representation of the tree is divided in two parts : the firt phase where lambda messages are send from the leaves to the root by level and the second phase where pi messages are send from the root to the leaves and computing the belief.

Tree structure for the first phase : 
The nodes are define this way:
FirstPhaseNode.
* level ; Double : the root has the level 0 then the next level of variables have 1 and so on.
* algoLevel ; Double : the algorithm level as the nodes have to send messages level by level
* state ; String : the state of a variable can be "root", "node" or "leaf"
* cpt ; Map[JointEvent, Probability] : the conditional probability table

cpt.

* JointEvent : It represents the joint values that can be taken by the variable and than the parent of this variable.

JoinEvent.

* Variable ; Double : the value of an occurrence of the variable
* Condition ; Double : the value of an occurrence of the condition

Probability : the corresponding probability of the JointEvent

Probability.

* value ; Double : the probability of the joint event

* lambda ; Map[Double, Probability] : the lambda message
* pi ; Map[Double, Probability] : the pi message
* activeNode ; Boolean : if the node has already sends a message or node

Tree structure for the second phase:
The nodes are defines this way:
SecondPhaseNode.

* state : the same as described before
* cpt : the same conditional probability table described before
* lambda ; Map[Double, Probability] : the lambda message
* pi ; Map[Double, Probability] : the pi message
* childrenLambda ; Map[VertexId, Map[Double, Probability]] : the lambda needed to compute the pi messages
* belief ; Map[Double, Probability] : the belief of the variable subject to the evidence.

The edges are simply Double. (no need of the edge attributes here)

### Mixture
MCL-B algorithm (to reduce the variance)

In order to create mixture, the former methods can be used. The mixture is created by boostrapping (resample without replacement) the content of the provided files. Then for each bootstrap sample, a tree is created. You may specified the size of the mixture. (to change the methods, replace in the mixture.scala file the methods used by the one you want to test, be warned that certain methods can be very computationally expensive)

Use the function createMixtureWithBootstrap(sparkContext, content, numberOfTree)
 
EM-MT algorithm (to reduce the bias)
 
To create a mixture that reduces the bias, the Expaction Maximization algorithm is used. 
 
Use the function EM(sc, content, sierOfMixture)
 
 
 






