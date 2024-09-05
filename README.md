This is an implementation of MapReduce framework from scratch to perform K-means clustering on a given dataset in a distributed manner.

The K-means algorithm is an iterative algorithm that partitions a dataset into K clusters. The algorithm proceeds as follows:

1. Randomly initialize K cluster centroids.
2. Assign each data point to the nearest cluster centroid.
3. Recompute the cluster centroids based on the mean of the data points assigned to each cluster.
4. Repeat steps 2 and 3 for a fixed number of iterations or until convergence (i.e., until the cluster centroids no longer change significantly).


<h2> Implementation Details: </h2>

<h3> Master </h3> The master program/process is responsible for running and communicating with the other components in the system.

<h3> Mapper </h3> Mapper read the input split by itself (based on the information provided by the master). Master should not send the input data points to the mapper.

<h3> Reducer </h3> The reducer will receive the intermediate key-value pairs from the mapper, perform the shuffle & sort function as mentioned,
and produce a set of final key-value pairs as output.
