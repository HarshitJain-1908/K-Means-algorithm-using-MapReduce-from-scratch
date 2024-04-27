import time
import grpc
from concurrent import futures
import sys
from mapper_pb2 import MapperResponse
from mapper_pb2_grpc import MapperServicer, add_MapperServicer_to_server
from mapper_pb2 import MapperDataResponse
from reducer_pb2_grpc import ReducerStub
import os
import logging
import random

def log(message):
    logging.info(message)

class MapperServicer(MapperServicer):
    def __init__(self, port, mapper_id):
        self.port = port
        self.mapper_id = mapper_id

        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=f'dump/M{self.mapper_id}_dump.txt',
                    filemode='w')
        
    def MapData(self, request, context):
        prob_failure = 0
        if random.random() < prob_failure:
            return MapperResponse(result="Failed to process shard data")
        # Implement the logic to process the shard data in the mapper
        log(f"Mapper {request.mapper_id} received shard data: {request.shard_file}, {request.start}, {request.end}")
        # Process the shard data and return the result
        self.Map(request.shard_file, request.start, request.end, request.centroids, request.R)
        
        # if request.mapper_id == 2:
        #     log("sleeping")
        #     time.sleep(4)

        return MapperResponse(result="Processed shard data")

    #Map function
    def Map(self, shard_file, start, end, centroids, R):

        kv_pairs = {}
        # log("centroids", centroids)
        with open(f"data/input/{shard_file}") as f:
            lines = f.readlines()
            lines = lines[start:end]
            for line in lines:
                # process the line
                nc_key = nearest_centroid(list(map(float, line.split(','))), centroids)
                if not nc_key in kv_pairs:
                    kv_pairs[nc_key] = []
                kv_pairs[nc_key].append(list(map(float, line.split(','))))

        self.Partition(kv_pairs, R)
        return
    
    def Partition(self, kv_pairs, R):
    
        # write the processed data to a new file
        if not os.path.exists(f"data/Mappers/M{self.mapper_id}"):
            os.makedirs(f"data/Mappers/M{self.mapper_id}")
        
        # delete all files in the directory
        for f in os.listdir(f"data/Mappers/M{self.mapper_id}"):
            os.remove(os.path.join(f"data/Mappers/M{self.mapper_id}", f))
        
        for r in range(1, R+1):
            open(f"data/Mappers/M{self.mapper_id}/partition_{r}.txt", "a")

        for k, values in kv_pairs.items():
            partition = k % R + 1
            with open(f"data/Mappers/M{self.mapper_id}/partition_{partition}.txt", "a") as f:
                for line in kv_pairs[k]:
                    f.write(str(k) + "," + str(line) + '\n')

    def Mapper2ReduceData(self, request, context):
        log(f"Mapper {self.mapper_id} send intermediate data to reducers")
        reducer_id = request.reducer_id
        partition_filename = f"data/Mappers/M{self.mapper_id}/partition_{reducer_id}.txt"
        
        response = MapperDataResponse()
        if os.path.exists(partition_filename):
            with open(partition_filename, "r") as f:
                for line in f:
                    key, value = line.strip().split(',', 1)
                    response.data.add(key=key, value=value)
            log(f"Data sent to reducer {reducer_id}")
        else:
            log(f"No data found for reducer {reducer_id}")
        
        return response

def nearest_centroid(coords, centroids):
    # log("coords", coords)
    nc_key = -1
    min_dist = 1e8
    for c in centroids:
        dist = ((c.x - coords[0])**2 + (c.y - coords[1])**2)**0.5
        if dist < min_dist:
            nc_key = c.centroid_id
            min_dist = dist
    # log("nc_key", nc_key)
    return nc_key
            
def serve(port):
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mapper = MapperServicer(port, int(port) - 6000)
        add_MapperServicer_to_server(mapper, server)
        server.add_insecure_port(f"[::]:{port}")
        
    except Exception as e:
        pass

    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    time.sleep(10)
    import sys
    serve(sys.argv[1].split(":")[1])