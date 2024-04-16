import subprocess
import argparse
import os
import grpc
from mapper_pb2 import ShardData, Centroid
from mapper_pb2_grpc import MapperStub
from reducer_pb2_grpc import ReducerStub
from reducer_pb2 import MapperInfo, Mapper
import concurrent.futures
import sys
import signal
import random

def create_shards(num_mappers):
    # read input file(s)
    num_input_files = len(os.listdir("data/input/"))
    
    shard_map = {}
    
    if num_input_files == 0:
        print("No input files found")
        return
    
    if num_input_files == 1:
        input_file = os.listdir("data/input/")[0]
        num_lines = sum(1 for line in open(f"data/input/{input_file}"))
        for i in range(num_mappers):
            start = i * num_lines // num_mappers
            end = (i + 1) * num_lines // num_mappers
            if i == num_mappers - 1:
                end = num_lines
            shard_map[i] = (input_file, start, end)
            
    else:
        # in case of multiple files, you cannot use the indices approach. You will have to distribute one file per mapper.
        input_files = os.listdir("data/input/")
        for i in range(num_mappers):
            num_lines = len(open(os.path.join("data", "input", input_files[i])).read().splitlines())
            shard_map[i] = (input_files[i], 0, num_lines)

    return shard_map



def start_map_phase(shard_map, centroids, num_reducers):
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to thread pool
        futures = {executor.submit(map_data, mapper_id, shard_file, start, end, centroids, num_reducers): mapper_id for mapper_id, (shard_file, start, end) in shard_map.items()}

        for future in concurrent.futures.as_completed(futures):
            mapper_id = futures[future]
            try:
                result = future.result()
                print(f"Mapper {mapper_id} response: {result}")
            except Exception as exc:
                print(f'Mapper {mapper_id} generated an exception: {exc}')

def map_data(mapper_id, shard_file, start, end, centroids, num_reducers):
    print(f"Master send to mapper {mapper_id}")
    with grpc.insecure_channel(f'localhost:{6000 + mapper_id}') as channel:
        stub = MapperStub(channel)
        return stub.MapData(ShardData(mapper_id = mapper_id, shard_file=shard_file, start=start, end=end, centroids = centroids, R = num_reducers)).result

# def start_map_phase(shard_map, centroids, num_reducers):
#     #can distribute work in parallel
#     for mapper_id, (shard_file, start, end) in shard_map.items():
#         print(f"Master send to mapper {mapper_id}")
#         with grpc.insecure_channel(f'localhost:{6000 + mapper_id}') as channel:
#             stub = MapperStub(channel)
#             response = stub.MapData(ShardData(mapper_id = mapper_id, shard_file=shard_file, start=start, end=end, centroids = centroids, R = num_reducers))
#             print(f"Mapper {mapper_id} response: {response.result}")

def start_reduce_phase(mappers, num_reducers):
    print("Starting reduce phase.")
    all_new_centroids = []
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to thread pool
        futures = {executor.submit(start_reduce, i, mappers): i for i in range(num_reducers)}

        for future in concurrent.futures.as_completed(futures):
            reducer_id = futures[future]
            try:
                result, new_centroids = future.result()
                print(f"Reducer {reducer_id} started reduce phase: {result}")
                all_new_centroids.extend(new_centroids)
            except Exception as exc:
                print(f'Reducer {reducer_id} generated an exception: {exc}')

    with open('centroids.txt', 'w') as f:
        for centroid in all_new_centroids:
            f.write(f'Centroid {centroid.centroid_id}: ({centroid.x}, {centroid.y})\n')

def start_reduce(reducer_id, mappers):
    with grpc.insecure_channel(f'localhost:{7000 + reducer_id}') as channel:
        stub = ReducerStub(channel)
        # Prepare the message with mappers' information
        reduce_request = MapperInfo()
        for ip, port in mappers:
            mapper_info = Mapper(ip=ip, port=port)
            reduce_request.mappers.append(mapper_info)
        # Send the message
        response = stub.StartReduce(reduce_request)
        return response.result, response.newcentroids
    
# def start_reduce_phase(mappers, num_reducers):
#     print("Starting reduce phase.")
#     all_new_centroids = []
#     for i in range(num_reducers):
#         with grpc.insecure_channel(f'localhost:{7000 + i}') as channel:
#             stub = ReducerStub(channel)
#             # Prepare the message with mappers' information
#             reduce_request = MapperInfo()
#             for ip, port in mappers:
#                 mapper_info = Mapper(ip=ip, port=port)
#                 reduce_request.mappers.append(mapper_info)
#             # Send the message
#             response = stub.StartReduce(reduce_request)
#             # print("NEW CENTROIDS", response.newcentroids)
#             # print("RESPONSE", response)
#             print(f"Reducer {i} started reduce phase: {response.result}")
#             all_new_centroids.extend(response.newcentroids)

#     with open('centroids.txt', 'w') as f:
#         for centroid in all_new_centroids:
#             f.write(f'Centroid {centroid.centroid_id}: ({centroid.x}, {centroid.y})\n')


def main(num_mappers, num_reducers, num_centroids):
    p = []
    centroids = []
    mappers = []

    for i in range(num_centroids):
        centroids.append(Centroid(centroid_id = i+1, x = random.random()*10, y = random.random()*10))
    print("centroids_main", centroids)
    for i in range(num_mappers):
        # s = subprocess.Popen("exec " + cmd, stdout=subprocess.PIPE, shell=True)
        # s = subprocess.Popen(["python3", "mapper.py", f"localhost:{6000 + i}"], stdout=subprocess.PIPE, shell=True)
        s = subprocess.Popen(["python3", "mapper.py", f"localhost:{6000 + i}"])
        mappers.append(('localhost', 6000 + i))
        p.append(s)
        print(f"Mapper {i} started with PID {s.pid}")
    for i in range(num_reducers):
        # s = subprocess.Popen(["python3", "reducer.py", f"localhost:{7000 + i}"], stdout=subprocess.PIPE, shell=True)
        s = subprocess.Popen(["python3", "reducer.py", f"localhost:{7000 + i}"])
        p.append(s)
        print(f"Reducer {i} started with PID {s.pid}")

    shutdown = False    
        
    while not shutdown:
        shard_map = create_shards(num_mappers)
        print(shard_map)
        start_map_phase(shard_map, centroids, num_reducers)
        start_reduce_phase(mappers, num_reducers)

        while True:
            try:
                pass
            except KeyboardInterrupt:
                for process in p:
                    print("terminating")
                    process.terminate()
                sys.exit(0)
        
        shutdown = True

if __name__ == "__main__":
    main(num_mappers = 4, num_reducers = 6, num_centroids = 3)