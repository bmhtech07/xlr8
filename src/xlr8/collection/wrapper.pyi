"""Type stubs for XLR8 Collection.

Inherits from PyMongoCollection for full IDE autocomplete.
Only overrides find() to return XLR8Cursor instead of PyMongo cursor.
"""

from typing import Any, Dict, List, Optional, Tuple

from pymongo.collection import Collection as PyMongoCollection

from .cursor import XLR8Cursor

class XLR8Collection(PyMongoCollection):
    """PyMongo-compatible collection with optional acceleration.

    Inherits all PyMongo collection methods. Only find() is overridden
    to return XLR8Cursor for accelerated DataFrame/Polars conversion.
    """

    def __init__(
        self,
        pymongo_collection: PyMongoCollection,
        schema: Optional[Any] = None,
        mongo_uri: Optional[str] = None,
        approx_document_size_bytes: int = 500,
    ) -> None: ...

    # Override find() to return XLR8Cursor
    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        sort: Optional[List[Tuple[str, int]]] = None,
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> XLR8Cursor: ...

    # XLR8-specific methods
    def raw_collection(self) -> PyMongoCollection: ...
    def set_schema(self, schema: Any) -> None: ...
    def get_schema(self) -> Optional[Any]: ...
    def clear_cache(self) -> None: ...

    # Properties
    @property
    def schema(self) -> Optional[Any]: ...
    @property
    def pymongo_collection(self) -> PyMongoCollection: ...
    @property
    def mongo_uri(self) -> Optional[str]: ...
    @property
    def approx_document_size_bytes(self) -> int: ...

def accelerate(
    pymongo_collection: PyMongoCollection,
    schema: Any,
    mongo_uri: Any,
    approx_document_size_bytes: int = 500,
) -> XLR8Collection: ...
