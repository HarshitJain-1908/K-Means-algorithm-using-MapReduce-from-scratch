import time
import grpc
from concurrent import futures
from reducer_pb2_grpc import ReducerServicer, add_ReducerServicer_to_server
from reducer_pb2 import ReducerResponse

class ReducerServicer(ReducerServicer):
    def __init__(self):
        self.data = {}

    def SendReduceData(self, request, context):
        key = request.key
        value = request.value
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)
        return ReducerResponse(result="Data added to reducer")

    def StartReduce(self, request, context):
        # Here you would handle the actual reduce logic
        # For simplicity, just print the collected data
        print("displaying data")
        print(self.data)
        print("---------------")
        # for key, values in self.data.items():
        #     print(f"Reduced data for key {key}: {self.reduce(values)}")
        # return ReducerResponse(result="Reduction completed")

    def reduce(self, values):
        # Placeholder reduce function (sum as an example)
        return sum(map(float, values))


# class ReducerServicer:
#     def __init__(self):
#         self.data = {}

#     def ReceiveData(self, request, context):
#         if request.key not in self.data:
#             self.data[request.key] = []
#         self.data[request.key].append(request.value)

#         return ReducerResponse(result="Received data")

#     def process_data(self):
#         # Sort and process the data
#         for key in sorted(self.data.keys()):
#             aggregated_value = self.aggregate(self.data[key])
#             print(f"Processed key {key} with value {aggregated_value}")

    # def Shuffle_and_Sort(self, request, context):

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ReducerServicer_to_server(ReducerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

def main():
    serve()

if __name__ == "__main__":
    main()