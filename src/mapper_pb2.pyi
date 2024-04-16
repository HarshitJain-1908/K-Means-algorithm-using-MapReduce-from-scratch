from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ShardData(_message.Message):
    __slots__ = ("mapper_id", "shard_file", "start", "end", "centroids", "R")
    MAPPER_ID_FIELD_NUMBER: _ClassVar[int]
    SHARD_FILE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    R_FIELD_NUMBER: _ClassVar[int]
    mapper_id: int
    shard_file: str
    start: int
    end: int
    centroids: _containers.RepeatedCompositeFieldContainer[Centroid]
    R: int
    def __init__(self, mapper_id: _Optional[int] = ..., shard_file: _Optional[str] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., centroids: _Optional[_Iterable[_Union[Centroid, _Mapping]]] = ..., R: _Optional[int] = ...) -> None: ...

class Centroid(_message.Message):
    __slots__ = ("centroid_id", "x", "y")
    CENTROID_ID_FIELD_NUMBER: _ClassVar[int]
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    centroid_id: int
    x: float
    y: float
    def __init__(self, centroid_id: _Optional[int] = ..., x: _Optional[float] = ..., y: _Optional[float] = ...) -> None: ...

class MapperResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: str
    def __init__(self, result: _Optional[str] = ...) -> None: ...
