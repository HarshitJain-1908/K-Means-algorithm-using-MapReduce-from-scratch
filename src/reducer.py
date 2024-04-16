import time
import grpc
from concurrent import futures
from reducer_pb2_grpc import ReducerServicer, add_ReducerServicer_to_server
from reducer_pb2 import ReducerResponse, Centroid
from mapper_pb2 import SendDataRequest
from reducer_pb2_grpc import Mapper2ReducerServiceStub, ReducerStub
from mapper_pb2_grpc import MapperStub
from google.protobuf.empty_pb2 import Empty
import os

class ReducerServicer(ReducerServicer):
    def __init__(self, port, reducer_id):
        self.data = {}
        self.port = port
        self.reducer_id = reducer_id

    # def Mapper2ReduceData(self, request, context):
    #     key = request.key
    #     value = request.value
    #     if key not in self.data:
    #         self.data[key] = []
    #     self.data[key].append(value)
    #     print(f"Reducer received data for key {key}: {value}")
    #     return ReducerResponse(result="Data added to reducer")

    def ReceiveDataFromMapper(self, mappers):
        print("hello")
        for m in mappers:
            ip = m.ip
            port = m.port
            channel = grpc.insecure_channel(f'{ip}:{port}')
            stub = MapperStub(channel)
            try:
                request = SendDataRequest(reducer_id=self.reducer_id)
                response = stub.Mapper2ReduceData(request)  # Assuming an Empty message triggers sending data
                for kv in response.data:
                    if kv.key not in self.data:
                        self.data[kv.key] = []
                    self.data[kv.key].append(kv.value)
                    print(f"Data received from mapper {ip}:{port} for key {kv.key}: {kv.value}")
            except grpc.RpcError as e:
                print(f"Failed to receive data from mapper {ip}:{port}: {str(e)}")
            finally:
                channel.close()


    def StartReduce(self, request, context):
        # Here you would handle the actual reduce logic
        # For simplicity, just print the collected data
        
        # for key, values in self.data.items():
        #     print(f"Reduced data for key {key}: {self.reduce(values)}")
        print("ok", request)
        
        open(f"data/Reducers/R{self.reducer_id}.txt", "w")
        
        self.ReceiveDataFromMapper(request.mappers)

        print("displaying data")
        print(self.data)
        print("---------------")
        
        
        if not(os.path.exists(f"data/Reducers")):
            os.makedirs(f"data/Reducers")    
         
        newcentroids = [] 
        for key, values in self.data.items():
            res = self.reduce(key, values)
            print(f"Reduced data for key {key}: {res}")
            
            newcentroids.append(Centroid(centroid_id=int(key), x=res[1][0], y=res[1][1]))
            
            with open(f"data/Reducers/R{self.reducer_id}.txt", "w") as f:
                f.write(str(res))
        
        
        return ReducerResponse(result="Reduction completed", newcentroids=newcentroids)

    def reduce(self, key, values):
        
        # values: list of strings, string : [2.0, 4.0]
        values = [v[1:-1].split(',') for v in values]
        print("values2", values)
        # Placeholder reduce function (sum as an example)
        #return sum(map(float, values))
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