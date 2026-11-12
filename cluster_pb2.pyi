from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Entry(_message.Message):
    __slots__ = ("index", "term", "key", "value", "noop")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    NOOP_FIELD_NUMBER: _ClassVar[int]
    index: int
    term: int
    key: str
    value: str
    noop: bool
    def __init__(self, index: _Optional[int] = ..., term: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., noop: bool = ...) -> None: ...

class VoteRequest(_message.Message):
    __slots__ = ("term", "candidate_id", "last_index", "last_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    candidate_id: int
    last_index: int
    last_term: int
    def __init__(self, term: _Optional[int] = ..., candidate_id: _Optional[int] = ..., last_index: _Optional[int] = ..., last_term: _Optional[int] = ...) -> None: ...

class VoteResponse(_message.Message):
    __slots__ = ("term", "granted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    granted: bool
    def __init__(self, term: _Optional[int] = ..., granted: bool = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("term", "follower_id", "last_index", "last_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    FOLLOWER_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    follower_id: int
    last_index: int
    last_term: int
    def __init__(self, term: _Optional[int] = ..., follower_id: _Optional[int] = ..., last_index: _Optional[int] = ..., last_term: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("term", "leader_id", "success", "retry_index", "entries", "commit_index")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    RETRY_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    term: int
    leader_id: int
    success: bool
    retry_index: int
    entries: _containers.RepeatedCompositeFieldContainer[Entry]
    commit_index: int
    def __init__(self, term: _Optional[int] = ..., leader_id: _Optional[int] = ..., success: bool = ..., retry_index: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[Entry, _Mapping]]] = ..., commit_index: _Optional[int] = ...) -> None: ...

class WriteRequest(_message.Message):
    __slots__ = ("key", "value", "crash_after_append")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CRASH_AFTER_APPEND_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    crash_after_append: bool
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ..., crash_after_append: bool = ...) -> None: ...

class WriteResponse(_message.Message):
    __slots__ = ("term", "leader_id", "committed", "index", "error")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    term: int
    leader_id: int
    committed: bool
    index: int
    error: str
    def __init__(self, term: _Optional[int] = ..., leader_id: _Optional[int] = ..., committed: bool = ..., index: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class ReadRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class ReadResponse(_message.Message):
    __slots__ = ("term", "found", "value", "commit_index")
    TERM_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    term: int
    found: bool
    value: str
    commit_index: int
    def __init__(self, term: _Optional[int] = ..., found: bool = ..., value: _Optional[str] = ..., commit_index: _Optional[int] = ...) -> None: ...

class PeerStatus(_message.Message):
    __slots__ = ("node_id", "active", "match_index")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    MATCH_INDEX_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    active: bool
    match_index: int
    def __init__(self, node_id: _Optional[int] = ..., active: bool = ..., match_index: _Optional[int] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("node_id", "role", "term", "leader_id", "commit_index", "last_index", "peers", "fetch_paused", "commit_paused", "truncations")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_INDEX_FIELD_NUMBER: _ClassVar[int]
    PEERS_FIELD_NUMBER: _ClassVar[int]
    FETCH_PAUSED_FIELD_NUMBER: _ClassVar[int]
    COMMIT_PAUSED_FIELD_NUMBER: _ClassVar[int]
    TRUNCATIONS_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    role: str
    term: int
    leader_id: int
    commit_index: int
    last_index: int
    peers: _containers.RepeatedCompositeFieldContainer[PeerStatus]
    fetch_paused: bool
    commit_paused: bool
    truncations: int
    def __init__(self, node_id: _Optional[int] = ..., role: _Optional[str] = ..., term: _Optional[int] = ..., leader_id: _Optional[int] = ..., commit_index: _Optional[int] = ..., last_index: _Optional[int] = ..., peers: _Optional[_Iterable[_Union[PeerStatus, _Mapping]]] = ..., fetch_paused: bool = ..., commit_paused: bool = ..., truncations: _Optional[int] = ...) -> None: ...

class ControlRequest(_message.Message):
    __slots__ = ("pause_fetch", "pause_commit")
    PAUSE_FETCH_FIELD_NUMBER: _ClassVar[int]
    PAUSE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    pause_fetch: bool
    pause_commit: bool
    def __init__(self, pause_fetch: bool = ..., pause_commit: bool = ...) -> None: ...
