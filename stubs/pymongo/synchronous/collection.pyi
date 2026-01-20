import bson
from _typeshed import Incomplete
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from bson.timestamp import Timestamp
from pymongo import (
    ASCENDING as ASCENDING,
    _csot,
    common as common,
    helpers_shared as helpers_shared,
    message as message,
)
from pymongo.collation import (
    Collation as Collation,
    validate_collation_or_none as validate_collation_or_none,
)
from pymongo.errors import (
    ConfigurationError as ConfigurationError,
    InvalidName as InvalidName,
    InvalidOperation as InvalidOperation,
    OperationFailure as OperationFailure,
)
from pymongo.operations import (
    DeleteMany as DeleteMany,
    DeleteOne as DeleteOne,
    IndexModel as IndexModel,
    InsertOne as InsertOne,
    ReplaceOne as ReplaceOne,
    SearchIndexModel as SearchIndexModel,
    UpdateMany as UpdateMany,
    UpdateOne as UpdateOne,
    _IndexKeyHint,
    _IndexList,
)
from pymongo.read_concern import (
    DEFAULT_READ_CONCERN as DEFAULT_READ_CONCERN,
    ReadConcern as ReadConcern,
)
from pymongo.read_preferences import ReadPreference as ReadPreference, _ServerMode
from pymongo.results import (
    BulkWriteResult as BulkWriteResult,
    DeleteResult as DeleteResult,
    InsertManyResult as InsertManyResult,
    InsertOneResult as InsertOneResult,
    UpdateResult as UpdateResult,
)
from pymongo.synchronous.change_stream import (
    CollectionChangeStream as CollectionChangeStream,
)
from pymongo.synchronous.client_session import ClientSession as ClientSession
from pymongo.synchronous.command_cursor import (
    CommandCursor as CommandCursor,
    RawBatchCommandCursor as RawBatchCommandCursor,
)
from pymongo.synchronous.cursor import (
    Cursor as Cursor,
    RawBatchCursor as RawBatchCursor,
)
from pymongo.synchronous.database import Database as Database
from pymongo.synchronous.pool import Connection as Connection
from pymongo.synchronous.server import Server as Server
from pymongo.typings import _CollationIn, _DocumentType, _DocumentTypeArg, _Pipeline
from pymongo.write_concern import (
    DEFAULT_WRITE_CONCERN as DEFAULT_WRITE_CONCERN,
    WriteConcern as WriteConcern,
    validate_boolean as validate_boolean,
)
from typing import (
    Any,
    Generic,
    Iterable,
    Mapping,
    MutableMapping,
    NoReturn,
    Sequence,
    TypeVar,
    overload,
)

T = TypeVar("T")

class ReturnDocument:
    BEFORE: bool
    AFTER: bool

class Collection(common.BaseObject, Generic[_DocumentType]):
    def __init__(
        self,
        database: Database[_DocumentType],
        name: str,
        create: bool | None = False,
        codec_options: CodecOptions[_DocumentTypeArg] | None = None,
        read_preference: _ServerMode | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> None: ...
    def __getattr__(self, name: str) -> Collection[_DocumentType]: ...
    def __getitem__(self, name: str) -> Collection[_DocumentType]: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def __bool__(self) -> NoReturn: ...
    @property
    def full_name(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def database(self) -> Database[_DocumentType]: ...
    @overload
    def with_options(
        self,
        codec_options: None = None,
        read_preference: _ServerMode | None = ...,
        write_concern: WriteConcern | None = ...,
        read_concern: ReadConcern | None = ...,
    ) -> Collection[_DocumentType]: ...
    @overload
    def with_options(
        self,
        codec_options: bson.CodecOptions[_DocumentTypeArg],
        read_preference: _ServerMode | None = ...,
        write_concern: WriteConcern | None = ...,
        read_concern: ReadConcern | None = ...,
    ) -> Collection[_DocumentTypeArg]: ...
    __iter__: Incomplete
    def __next__(self) -> NoReturn: ...
    next = __next__
    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn: ...
    def watch(
        self,
        pipeline: _Pipeline | None = None,
        full_document: str | None = None,
        resume_after: Mapping[str, Any] | None = None,
        max_await_time_ms: int | None = None,
        batch_size: int | None = None,
        collation: _CollationIn | None = None,
        start_at_operation_time: Timestamp | None = None,
        session: ClientSession | None = None,
        start_after: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        full_document_before_change: str | None = None,
        show_expanded_events: bool | None = None,
    ) -> CollectionChangeStream[_DocumentType]: ...
    @_csot.apply
    def bulk_write(
        self,
        requests: Sequence[_WriteOp[_DocumentType]],
        ordered: bool = True,
        bypass_document_validation: bool | None = None,
        session: ClientSession | None = None,
        comment: Any | None = None,
        let: Mapping[str, Any] | None = None,
    ) -> BulkWriteResult: ...
    def insert_one(
        self,
        document: _DocumentType | RawBSONDocument,
        bypass_document_validation: bool | None = None,
        session: ClientSession | None = None,
        comment: Any | None = None,
    ) -> InsertOneResult: ...
    @_csot.apply
    def insert_many(
        self,
        documents: Iterable[_DocumentType | RawBSONDocument],
        ordered: bool = True,
        bypass_document_validation: bool | None = None,
        session: ClientSession | None = None,
        comment: Any | None = None,
    ) -> InsertManyResult: ...
    def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool | None = None,
        collation: _CollationIn | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        sort: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult: ...
    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any] | _Pipeline,
        upsert: bool = False,
        bypass_document_validation: bool | None = None,
        collation: _CollationIn | None = None,
        array_filters: Sequence[Mapping[str, Any]] | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        sort: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult: ...
    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any] | _Pipeline,
        upsert: bool = False,
        array_filters: Sequence[Mapping[str, Any]] | None = None,
        bypass_document_validation: bool | None = None,
        collation: _CollationIn | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult: ...
    def drop(
        self,
        session: ClientSession | None = None,
        comment: Any | None = None,
        encrypted_fields: Mapping[str, Any] | None = None,
    ) -> None: ...
    def delete_one(
        self,
        filter: Mapping[str, Any],
        collation: _CollationIn | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> DeleteResult: ...
    def delete_many(
        self,
        filter: Mapping[str, Any],
        collation: _CollationIn | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> DeleteResult: ...
    def find_one(
        self, filter: Any | None = None, *args: Any, **kwargs: Any
    ) -> _DocumentType | None: ...
    def find(self, *args: Any, **kwargs: Any) -> Cursor[_DocumentType]: ...
    def find_raw_batches(
        self, *args: Any, **kwargs: Any
    ) -> RawBatchCursor[_DocumentType]: ...
    def estimated_document_count(
        self, comment: Any | None = None, **kwargs: Any
    ) -> int: ...
    def count_documents(
        self,
        filter: Mapping[str, Any],
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> int: ...
    def create_indexes(
        self,
        indexes: Sequence[IndexModel],
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> list[str]: ...
    def create_index(
        self,
        keys: _IndexKeyHint,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> str: ...
    def drop_indexes(
        self,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    @_csot.apply
    def drop_index(
        self,
        index_or_name: _IndexKeyHint,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    def list_indexes(
        self, session: ClientSession | None = None, comment: Any | None = None
    ) -> CommandCursor[MutableMapping[str, Any]]: ...
    def index_information(
        self, session: ClientSession | None = None, comment: Any | None = None
    ) -> MutableMapping[str, Any]: ...
    def list_search_indexes(
        self,
        name: str | None = None,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> CommandCursor[Mapping[str, Any]]: ...
    def create_search_index(
        self,
        model: Mapping[str, Any] | SearchIndexModel,
        session: ClientSession | None = None,
        comment: Any = None,
        **kwargs: Any,
    ) -> str: ...
    def create_search_indexes(
        self,
        models: list[SearchIndexModel],
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> list[str]: ...
    def drop_search_index(
        self,
        name: str,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    def update_search_index(
        self,
        name: str,
        definition: Mapping[str, Any],
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> None: ...
    def options(
        self, session: ClientSession | None = None, comment: Any | None = None
    ) -> MutableMapping[str, Any]: ...
    def aggregate(
        self,
        pipeline: _Pipeline,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> CommandCursor[_DocumentType]: ...
    def aggregate_raw_batches(
        self,
        pipeline: _Pipeline,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> RawBatchCursor[_DocumentType]: ...
    @_csot.apply
    def rename(
        self,
        new_name: str,
        session: ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> MutableMapping[str, Any]: ...
    def distinct(
        self,
        key: str,
        filter: Mapping[str, Any] | None = None,
        session: ClientSession | None = None,
        comment: Any | None = None,
        hint: _IndexKeyHint | None = None,
        **kwargs: Any,
    ) -> list[Any]: ...
    def find_one_and_delete(
        self,
        filter: Mapping[str, Any],
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        sort: _IndexList | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> _DocumentType | None: ...
    def find_one_and_replace(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        sort: _IndexList | None = None,
        upsert: bool = False,
        return_document: bool = ...,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> _DocumentType | None: ...
    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any] | _Pipeline,
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        sort: _IndexList | None = None,
        upsert: bool = False,
        return_document: bool = ...,
        array_filters: Sequence[Mapping[str, Any]] | None = None,
        hint: _IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> _DocumentType | None: ...
