import time
import grpc
from concurrent import futures
from reducer_pb2_grpc import ReducerServicer, add_ReducerServicer_to_server
from reducer_pb2 import ReducerResponse
from mapper_pb2 import Centroid
from mapper_pb2 import SendDataRequest
from reducer_pb2_grpc import Mapper2ReducerServiceStub, ReducerStub
from mapper_pb2_grpc import MapperStub
from google.protobuf.empty_pb2 import Empty
import os
import logging
import random


def log(message):
    logging.info(message)
    
class ReducerServicer(ReducerServicer):
    def __init__(self, port, reducer_id):
        self.data = {}
        self.port = port
        self.reducer_id = reducer_id

        # Set up logging

        if not(os.path.exists(f"dump")):
            os.makedirs(f"dump")

        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=f'dump/R{self.reducer_id}_dump.txt',
                    filemode='w')

    def ReceiveDataFromMapper(self, mappers):
        for m in mappers:
            ip = m.ip
            port = m.port
            channel = grpc.insecure_channel(f'{ip}:{port}')
            stub = MapperStub(channel)
            try:
                request = SendDataRequest(reducer_id=self.reducer_id)
                response = stub.Mapper2ReduceData(request)  # Assuming an Empty message triggers sending data
                
                #shuffling and sorting
                for kv in response.data:
                    if kv.key not in self.data:
                        self.data[kv.key] = []
                    self.data[kv.key].append(kv.value)
                    log(f"Data received from mapper {ip}:{port} for key {kv.key}: {kv.value}")
                    
            except grpc.RpcError as e:
                log(f"Failed to receive data from mapper {ip}:{port}: {str(e)}")
            finally:
                channel.close()


    def StartReduce(self, request, context):
        
        prob_failure = 0.1
        if random.random() < prob_failure:
            return ReducerResponse(result="Failed to start reduce phase", newcentroids=[])

        log(f"Reducer {self.reducer_id} started reduce phase")

        # Create a file to store the reduced data
        with open(f"data/Reducers/R{self.reducer_id}.txt", "w") as f:
            f.write("")
        
        if not(os.path.exists(f"data/Reducers")):
            os.makedirs(f"data/Reducers")

        open(f"data/Reducers/R{self.reducer_id}.txt", "w")
        
        self.ReceiveDataFromMapper(request.mappers)

        log("displaying data")
        log(self.data)
         
        newcentroids = [] 
        for key, values in self.data.items():
            res = self.reduce(key, values)
            log(f"Reduced data for key {key}: {res}")
            
            newcentroids.append(Centroid(centroid_id=int(key), x=res[1][0], y=res[1][1]))
            
            with open(f"data/Reducers/R{self.reducer_id}.txt", "a") as f:
                f.write(str(res) + '\n')
        
        return ReducerResponse(result="Reduction completed", newcentroids=newcentroids)

    #Reduce function
    def reduce(self, key, values):
        
        # values: list of strings, string : [2.0, 4.0]
        values = [v[1:-1].split(',') for v in values]
        log(values)

        values = [list(map(float, v)) for v in values]
        
        mean1 = sum([v[0] for v in values]) / len(values)
        mean2 = sum([v[1] for v in values]) / len(values) 
        return (key, [mean1, mean2])

    # def Shuffle_and_Sort(self, request, context):

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer = ReducerServicer(port, int(port) - 7000)
    add_ReducerServicer_to_server(reducer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    import sys
    serve(sys.argv[1].split(":")[1])