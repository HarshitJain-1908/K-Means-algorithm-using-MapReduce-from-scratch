import time
import grpc
from concurrent import futures
from reducer_pb2_grpc import ReducerServicer, add_ReducerServicer_to_server
from reducer_pb2 import ReducerResponse
from reducer_pb2_grpc import Mapper2ReducerServiceStub
from google.protobuf.empty_pb2 import Empty

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
        for ip, port in mappers:
            channel = grpc.insecure_channel(f'{ip}:{port}')
            stub = Mapper2ReducerServiceStub(channel)
            try:
                response = stub.Mapper2ReduceData(reducer_id=self.reducer_id)  # Assuming an Empty message triggers sending data
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
        print("ok", request.MapperInfo)
        self.ReceiveDataFromMapper(request.MapperInfo)

        print("displaying data")
        print(self.data)
        print("---------------")
        for key, values in self.data.items():
            print(f"Reduced data for key {key}: {self.reduce(values)}")
        return ReducerResponse(result="Reduction completed")

    def reduce(self, values):
        # Placeholder reduce function (sum as an example)
        return sum(map(float, values))


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