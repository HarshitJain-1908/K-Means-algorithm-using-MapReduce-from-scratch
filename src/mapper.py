import time
import grpc
from concurrent import futures
import sys
from mapper_pb2 import MapperResponse
from mapper_pb2_grpc import MapperServicer, add_MapperServicer_to_server
import os

class MapperServicer(MapperServicer):
    def MapData(self, request, context):
        print(f"request is {request}")
        # Implement the logic to process the shard data in the mapper
        print(f"Mapper {request.mapper_id} received shard data: {request.shard_file}, {request.start}, {request.end}")
        # Process the shard data and return the result
        
        self.Map(request.shard_file, request.start, request.end, request.centroids)
        
        return MapperResponse(result="Processed shard data")
    
    def Map(self, shard_file, start, end, centroids, R):

        kv_pairs = {}
        print("centroids", centroids)
        with open(f"Data/Input/{shard_file}") as f:
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
        if not os.path.exists(f"Data/Mappers/M{self.port}"):
            os.makedirs(f"Data/Mappers/M{self.port}")
        
        # delete all files in the directory
        for f in os.listdir(f"Data/Mappers/M{self.port}"):
            os.remove(os.path.join(f"Data/Mappers/M{self.port}", f))
        
        for r in range(0, R):
            open(f"Data/Mappers/M{self.port}/partition_{partition}.txt", "a")

        for k in kv_pairs:
            partition = k % 2
            with open(f"Data/Mappers/M{self.port}/partition_{partition}.txt", "a") as f:
                for line in kv_pairs[k]:
                    f.write(str(k) + "," + str(line) + '\n')
    

    def __init__(self, port):
        self.port = port

def nearest_centroid(coords, centroids):
    print("coords", coords)
    nc_key = -1
    min_dist = 1e8
    for c in centroids:
        dist = ((c.x - coords[0])**2 + (c.y - coords[1])**2)**0.5
        if dist < min_dist:
            nc_key = c.centroid_id
            min_dist = dist
    print("nc_key", nc_key)
    return nc_key
            
def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper = MapperServicer(int(port) - 6000)
    add_MapperServicer_to_server(mapper, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    #time.sleep(3)
    import sys
    serve(sys.argv[1].split(":")[1])