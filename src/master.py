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
import logging


if not(os.path.exists(f"dump")):
    os.makedirs(f"dump")

# Set up logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='dump/dump.txt',
                    filemode='w')

def log(message):
    logging.info(message)

def create_shards(num_mappers):
    # read input file(s)
    num_input_files = len(os.listdir("data/input/"))
    
    shard_map = {}
    
    if num_input_files == 0:
        log("No input files found")
        return
    
    if num_input_files == 1:
        input_file = os.listdir("data/input/")[0]
        num_lines = sum(1 for line in open(f"data/input/{input_file}"))
        for i in range(num_mappers):
            start = i * num_lines // num_mappers
            end = (i + 1) * num_lines // num_mappers
            if i == num_mappers - 1:
                end = num_lines
            shard_map[i+1] = (input_file, start, end)
            
    else:
        # in case of multiple files, you cannot use the indices approach. You will have to distribute one file per mapper.
        input_files = os.listdir("data/input/")
        for i in range(num_mappers):
            num_lines = len(open(os.path.join("data", "input", input_files[i])).read().splitlines())
            shard_map[i+1] = (input_files[i], 0, num_lines)

    log(f"Shard map created: {shard_map}")

    return shard_map

def start_map_phase(shard_map, centroids, num_reducers):
    log("Starting map phase...")
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to thread pool
        futures = {executor.submit(map_data, mapper_id, shard_file, start, end, centroids, num_reducers): mapper_id for mapper_id, (shard_file, start, end) in shard_map.items()}

        results = {}
        for future in concurrent.futures.as_completed(futures):
            mapper_id = futures[future]
            try:
                result = future.result()
                results[mapper_id] = result
                log(f"Mapper {mapper_id} response: {result}")
            except Exception as exc:
                log(f'Mapper {mapper_id} generated an exception: {exc}')
        return results

def map_data(mapper_id, shard_file, start, end, centroids, num_reducers):
    log(f"Master sends to mapper {mapper_id}")
    with grpc.insecure_channel(f'localhost:{6000 + mapper_id}') as channel:
        stub = MapperStub(channel)
        return stub.MapData(ShardData(mapper_id = mapper_id, shard_file=shard_file, start=start, end=end, centroids = centroids, R = num_reducers)).result

def start_reduce_phase(mappers, num_reducers, og_centroids):
    log("Starting reduce phase...")
    all_new_centroids = []
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to thread pool
        futures = {executor.submit(start_reduce, i+1, mappers): i for i in range(num_reducers)}

        for future in concurrent.futures.as_completed(futures):
            reducer_id = futures[future]
            try:
                result, new_centroids = future.result()
                all_new_centroids.extend(new_centroids)
                log(f"Reducer {reducer_id} started reduce phase: {result}")
            except Exception as exc:
                log(f'Reducer {reducer_id} generated an exception: {exc}')

    for centroid in og_centroids:
        if not any(c.centroid_id == centroid.centroid_id for c in all_new_centroids):
            all_new_centroids.append(centroid)

    all_new_centroids.sort(key=lambda centroid: centroid.centroid_id)

    with open('centroids.txt', 'w') as f:
        for centroid in all_new_centroids:
            f.write(f'Centroid {centroid.centroid_id}: ({centroid.x}, {centroid.y})\n')

    return all_new_centroids

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

def has_converged(old_centroids, new_centroids, tolerance=0.01):
    return all(
        (abs(o.x - n.x) ** 2 + abs(o.y - n.y) ** 2) ** 0.5 < tolerance
        for o, n in zip(old_centroids, new_centroids)
    )

def main(num_mappers, num_reducers, num_centroids, max_iterations):
    p = []   
    mappers = []

    for i in range(num_mappers):
        # s = subprocess.Popen("exec " + cmd, stdout=subprocess.PIPE, shell=True)
        # s = subprocess.Popen(["python3", "mapper.py", f"localhost:{6000 + i}"], stdout=subprocess.PIPE, shell=True)
        s = subprocess.Popen(["python3", "mapper.py", f"localhost:{6001 + i}"])
        mappers.append(('localhost', 6001 + i))
        p.append(s)
        log(f"Mapper {i} started with PID {s.pid}")
    for i in range(num_reducers):
        # s = subprocess.Popen(["python3", "reducer.py", f"localhost:{7000 + i}"], stdout=subprocess.PIPE, shell=True)
        s = subprocess.Popen(["python3", "reducer.py", f"localhost:{7001 + i}"])
        p.append(s)
        log(f"Reducer {i} started with PID {s.pid}")

    centroids = []

    for i in range(num_centroids):
        centroids.append(Centroid(centroid_id = i+1, x = random.random()*10, y = random.random()*10))
    
    log("Initial random centroids: " + ", ".join(f"Centroid {c.centroid_id}: ({c.x}, {c.y})" for c in centroids))

    iteration = 0
    while iteration < max_iterations:
        try:
            log(f"Iteration {iteration + 1} begins")
            shard_map = create_shards(num_mappers)

            # Map phase
            start_map_phase(shard_map, centroids, num_reducers)

            # Reduce phase
            new_centroids = start_reduce_phase(mappers, num_reducers, centroids)

            log("New centroids: " + ", ".join(f"Centroid {c.centroid_id}: ({c.x}, {c.y})" for c in new_centroids))

            # Check for convergence or update centroids
            if has_converged(centroids, new_centroids, tolerance=0.01):
                log("Convergence reached.")
                print("Convergence reached")
                for process in p:
                    process.terminate()
                break

            #updating input centroids for the next iteration
            centroids = new_centroids
            iteration += 1
            
            
            
        except KeyboardInterrupt:
                for process in p:
                    print("terminating")
                    process.terminate()
                sys.exit(0)
                
    for process in p:
        process.terminate()

if __name__ == "__main__":
    main(num_mappers = 4, num_reducers = 6, num_centroids = 3, max_iterations = 10)