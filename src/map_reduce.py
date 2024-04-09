import os
import random
import math
import grpc
import concurrent.futures
import itertools
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List

# Protobuf definitions
import km_pb2
import km_pb2_grpc

# Data point representation
@dataclass
class DataPoint:
    id: int
    features: List[float]

# Master server
class KMeansServer(km_pb2_grpc.KMeansServicer):
    def __init__(self, num_mappers, num_reducers, num_centroids, num_iterations):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_centroids = num_centroids
        self.num_iterations = num_iterations
        self.centroids = []
        self.input_points = []
        self.mapper_channels = []
        self.reducer_channels = []

    def LoadInputData(self, filename):
        # Load input data points from a file
        with open(filename, 'r') as f:
            for line in f:
                features = [float(x) for x in line.strip().split(',')]
                self.input_points.append(DataPoint(len(self.input_points), features))
        print(f"Loaded {len(self.input_points)} input data points.")

    def InputSplit(self, request, context):
        # Split the input data into smaller chunks for the mappers
        chunk_size = len(self.input_points) // self.num_mappers
        for i in range(self.num_mappers):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < self.num_mappers - 1 else len(self.input_points)
            print(f"Splitting input data for Mapper {i+1}")
            yield km_pb2.InputSplitResponse(
                split_id=i,
                data_points=[
                    km_pb2.DataPoint(
                        id=point.id,
                        features=point.features
                    ) for point in self.input_points[start:end]
                ]
            )

    def CentroidCompilation(self, request, context):
        # Compile the final list of centroids from the reducer outputs
        centroids = []
        for i in range(self.num_reducers):
            with open(f'Reducers/R{i+1}.txt', 'r') as f:
                print(f"Reading centroids from Reducer {i+1} output file")
                for line in f:
                    centroid_id, *features = line.strip().split(',')
                    centroids.append(DataPoint(int(centroid_id), [float(x) for x in features]))
        return km_pb2.CentroidResponse(centroids=centroids)

    def Run(self):
        self.LoadInputData('Data/Input/points.txt')
        self.centroids = self.initialize_centroids(self.num_centroids)
        print(f"Iteration 0: Randomly initialized centroids: {self.centroids}")

        for iteration in range(1, self.num_iterations + 1):
            print(f"Iteration {iteration}:")

            # Invoke mappers
            print("Invoking mappers...")
            self.invoke_mappers()

            # Invoke reducers
            print("Invoking reducers...")
            self.invoke_reducers()

            # Compile the final centroids
            self.centroids = self.CentroidCompilation(None, None).centroids
            print(f"New centroids: {self.centroids}")

    def initialize_centroids(self, num_centroids):
        # Randomly select centroids from the input data points
        print(f"Initializing {num_centroids} centroids randomly.")
        return random.sample(self.input_points, num_centroids)

    def invoke_mappers(self):
        # Invoke the mappers and collect the results
        with ThreadPoolExecutor(max_workers=self.num_mappers) as executor:
            futures = []
            for i in range(self.num_mappers):
                futures.append(executor.submit(self.run_mapper, i))
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def run_mapper(self, mapper_id):
        # Establish gRPC connection with the mapper
        print(f"Mapper {mapper_id+1} starting...")
        with grpc.insecure_channel(f'localhost:{50051 + mapper_id}') as channel:
            stub = km_pb2_grpc.MapperStub(channel)

            # Get the input split from the master
            input_split = list(self.InputSplit(None, None))[mapper_id]

            # Perform the mapping and partitioning
            partitions = self.map_and_partition(input_split, self.centroids)

            # Send the partitions to the reducers
            self.shuffle_and_sort(partitions, mapper_id)

        print(f"Mapper {mapper_id+1} finished.")
        return True

    def map_and_partition(self, input_split, centroids):
        # Implement the mapping and partitioning logic
        partitions = [[] for _ in range(self.num_reducers)]
        for point in input_split.data_points:
            nearest_centroid_id = self.find_nearest_centroid(point, centroids)
            partitions[nearest_centroid_id % self.num_reducers].append(
                km_pb2.KeyValuePair(
                    key=nearest_centroid_id,
                    value=km_pb2.DataPoint(
                        id=point.id,
                        features=point.features
                    )
                )
            )
        return partitions

    def shuffle_and_sort(self, partitions, mapper_id):
        # Implement the shuffling and sorting logic
        # and send the partitions to the reducers
        for i, partition in enumerate(partitions):
            with grpc.insecure_channel(f'localhost:{50051 + self.num_mappers + i}') as channel:
                stub = km_pb2_grpc.ReducerStub(channel)
                print(f"Mapper {mapper_id+1} sending partition {i+1} to Reducer {i+1}")
                response = stub.ShuffleAndSort(km_pb2.PartitionRequest(
                    mapper_id=mapper_id,
                    partition_id=i,
                    key_value_pairs=partition
                ))
                print(f"Mapper {mapper_id+1} sent partition {i+1} to Reducer {i+1}: {response.status}")

    def find_nearest_centroid(self, point, centroids):
        # Implement the logic to find the nearest centroid for a data point
        min_distance = float('inf')
        nearest_centroid_id = -1
        for i, centroid in enumerate(centroids):
            distance = self.euclidean_distance(point.features, centroid.features)
            if distance < min_distance:
                min_distance = distance
                nearest_centroid_id = i
        return nearest_centroid_id

    def euclidean_distance(self, a, b):
        # Compute the Euclidean distance between two vectors
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    def invoke_reducers(self):
        # Invoke the reducers and collect the results
        with ThreadPoolExecutor(max_workers=self.num_reducers) as executor:
            futures = []
            for i in range(self.num_reducers):
                futures.append(executor.submit(self.run_reducer, i))
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def run_reducer(self, reducer_id):
        # Establish gRPC connection with the reducer
        print(f"Reducer {reducer_id+1} starting...")
        with grpc.insecure_channel(f'localhost:{50051 + self.num_mappers + reducer_id}') as channel:
            stub = km_pb2_grpc.ReducerStub(channel)

            # Perform the shuffling, sorting, and reduction
            centroids = self.reduce(stub, reducer_id)

            # Save the reducer output to a file
            print(f"Saving centroids from Reducer {reducer_id+1} to file")
            with open(f'Reducers/R{reducer_id+1}.txt', 'w') as f:
                for centroid in centroids:
                    f.write(f"{centroid.id},{','.join(map(str, centroid.features))}\n")

        print(f"Reducer {reducer_id+1} finished.")
        return True

    def reduce(self, stub, reducer_id):
        # Implement the reduction logic
        centroids = []
        for mapper_id in range(self.num_mappers):
            print(f"Reducer {reducer_id+1} processing data from Mapper {mapper_id+1}")
            response = stub.Reduce(km_pb2.ReduceRequest(
                mapper_id=mapper_id,
                reducer_id=reducer_id
            ))
            for centroid in response.centroids:
                centroids.append(DataPoint(
                    id=centroid.id,
                    features=list(centroid.features)
                ))
        return centroids

def serve():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    km_pb2_grpc.add_KMeansServicer_to_server(KMeansServer(num_mappers=4, num_reducers=2, num_centroids=3, num_iterations=5), server)

    # Start the mappers and reducers
    for i in range(4):
        server.add_insecure_port(f'[::]:{50051 + i}')
        km_pb2_grpc.add_MapperServicer_to_server(MapperServer(), server)
    for i in range(2):
        server.add_insecure_port(f'[::]:{50051 + 4 + i}')
        km_pb2_grpc.add_ReducerServicer_to_server(ReducerServer(), server)

    print("Starting the server...")
    server.start()
    server.wait_for_termination()

# Mapper server
class MapperServer(km_pb2_grpc.MapperServicer):
    def Map(self, request, context):
        # Implement the mapping logic
        partitions = self.map_and_partition(request, self.centroids)
        return km_pb2.Empty()

    def Partition(self, request, context):
        # Implement the partitioning logic
        partitions = [[] for _ in range(self.num_reducers)]
        for key_value_pair in request.key_value_pairs:
            partitions[key_value_pair.key % self.num_reducers].append(key_value_pair)
        return km_pb2.PartitionResponse(status="SUCCESS")

    def map_and_partition(self, input_split, centroids):
        # Implement the mapping and partitioning logic
        partitions = [[] for _ in range(self.num_reducers)]
        for point in input_split.data_points:
            nearest_centroid_id = self.find_nearest_centroid(point, centroids)
            partitions[nearest_centroid_id % self.num_reducers].append(
                km_pb2.KeyValuePair(
                    key=nearest_centroid_id,
                    value=km_pb2.DataPoint(
                        id=point.id,
                        features=point.features
                    )
                )
            )
        return partitions

    def find_nearest_centroid(self, point, centroids):
        # Implement the logic to find the nearest centroid for a data point
        min_distance = float('inf')
        nearest_centroid_id = -1
        for i, centroid in enumerate(centroids):
            distance = self.euclidean_distance(point.features, centroid.features)
            if distance < min_distance:
                min_distance = distance
                nearest_centroid_id = i
        return nearest_centroid_id

    def euclidean_distance(self, a, b):
        # Compute the Euclidean distance between two vectors
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

# Reducer server
class ReducerServer(km_pb2_grpc.ReducerServicer):
    def ShuffleAndSort(self, request, context):
        # Implement the shuffling and sorting logic
        key_value_pairs = list(request.key_value_pairs)
        key_value_pairs.sort(key=lambda x: x.key)
        return km_pb2.PartitionResponse(status="SUCCESS")

    def Reduce(self, request, context):
        # Implement the reduction logic
        centroids = []
        for key, group in itertools.groupby(request.key_value_pairs, lambda x: x.key):
            data_points = [DataPoint(id=p.value.id, features=p.value.features) for p in group]
            new_centroid = DataPoint(
                id=key,
                features=[sum(x) / len(data_points) for x in zip(*[p.features for p in data_points])]
            )
            centroids.append(new_centroid)
        return km_pb2.CentroidResponse(centroids=centroids)


if __name__ == '__main__':
    server = KMeansServer(num_mappers=4, num_reducers=2, num_centroids=3, num_iterations=5)
    server.LoadInputData('Data/Input/points.txt')
    server.Run()
    serve()