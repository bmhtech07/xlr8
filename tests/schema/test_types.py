"""
Unit tests for XLR8 type definitions.

Tests all primitive and complex types including:
- Primitive types (String, Int, Float, Bool, Timestamp, ObjectId)
- Complex types (Struct, List, Any)
- Arrow schema conversion
- Type equality and hashing
"""

import pytest
import pyarrow as pa
from xlr8.schema import types as Types

class TestPrimitiveTypes:
    """Tests for primitive type definitions."""

    def test_string_type(self):
        """Test String type creation and Arrow conversion."""
        string_type = Types.String()
        assert string_type.to_arrow() == pa.string()
        assert repr(string_type) == "String()"
        assert string_type == Types.String()
        assert hash(string_type) == hash(Types.String())

    def test_int_type(self):
        """Test Int type (always 64-bit)."""
        int_type = Types.Int()
        assert int_type.to_arrow() == pa.int64()
        assert repr(int_type) == "Int()"
        assert int_type == Types.Int()

    def test_float_type(self):
        """Test Float type (always 64-bit)."""
        float_type = Types.Float()
        assert float_type.to_arrow() == pa.float64()
        assert repr(float_type) == "Float()"
        assert float_type == Types.Float()

    def test_bool_type(self):
        """Test Bool type."""
        bool_type = Types.Bool()
        assert bool_type.to_arrow() == pa.bool_()
        assert repr(bool_type) == "Bool()"
        assert bool_type == Types.Bool()

    def test_objectid_type(self):
        """Test ObjectId type (stored as string)."""
        objectid_type = Types.ObjectId()
        assert objectid_type.to_arrow() == pa.string()
        assert repr(objectid_type) == "ObjectId()"
        assert objectid_type == Types.ObjectId()


class TestTimestampType:
    """Tests for Timestamp type with various configurations."""

    def test_timestamp_default(self):
        """Test Timestamp with default values (ns, UTC)."""
        ts = Types.Timestamp()
        assert ts.unit == "ns"
        assert ts.tz == "UTC"
        assert ts.to_arrow() == pa.timestamp("ns", tz="UTC")

    def test_timestamp_milliseconds(self):
        """Test Timestamp with millisecond precision."""
        ts = Types.Timestamp("ms", tz="UTC")
        assert ts.unit == "ms"
        assert ts.tz == "UTC"
        assert ts.to_arrow() == pa.timestamp("ms", tz="UTC")

    def test_timestamp_microseconds(self):
        """Test Timestamp with microsecond precision."""
        ts = Types.Timestamp("us", tz="America/New_York")
        assert ts.unit == "us"
        assert ts.tz == "America/New_York"
        assert ts.to_arrow() == pa.timestamp("us", tz="America/New_York")

    def test_timestamp_seconds(self):
        """Test Timestamp with second precision."""
        ts = Types.Timestamp("s", tz="Europe/London")
        assert ts.unit == "s"
        assert ts.tz == "Europe/London"
        assert ts.to_arrow() == pa.timestamp("s", tz="Europe/London")

    def test_timestamp_naive(self):
        """Test Timestamp without timezone."""
        ts = Types.Timestamp("ms", tz=None)
        assert ts.unit == "ms"
        assert ts.tz is None
        assert ts.to_arrow() == pa.timestamp("ms", tz=None)

    def test_timestamp_equality(self):
        """Test Timestamp equality comparison."""
        ts1 = Types.Timestamp("ms", tz="UTC")
        ts2 = Types.Timestamp("ms", tz="UTC")
        ts3 = Types.Timestamp("ns", tz="UTC")
        ts4 = Types.Timestamp("ms", tz="America/New_York")
        
        assert ts1 == ts2
        assert ts1 != ts3  # Different unit
        assert ts1 != ts4  # Different timezone

    def test_timestamp_immutable(self):
        """Test that Timestamp is immutable (frozen dataclass)."""
        ts = Types.Timestamp("ms", tz="UTC")
        with pytest.raises(Exception):  # FrozenInstanceError
            ts.unit = "ns"


class TestStructType:
    """Tests for Struct type (nested documents)."""

    def test_struct_simple(self):
        """Test Struct with simple fields."""
        struct_type = Types.Struct({
            "name": Types.String(),
            "age": Types.Int(),
            "active": Types.Bool()
        })
        
        arrow_type = struct_type.to_arrow()
        assert pa.types.is_struct(arrow_type)
        assert len(arrow_type) == 3
        assert arrow_type.field("name").type == pa.string()
        assert arrow_type.field("age").type == pa.int64()
        assert arrow_type.field("active").type == pa.bool_()

    def test_struct_nested(self):
        """Test Struct with nested Struct."""
        struct_type = Types.Struct({
            "user": Types.Struct({
                "id": Types.Int(),
                "email": Types.String()
            }),
            "timestamp": Types.Timestamp("ms", tz="UTC")
        })
        
        arrow_type = struct_type.to_arrow()
        assert pa.types.is_struct(arrow_type)
        assert pa.types.is_struct(arrow_type.field("user").type)

    def test_struct_equality(self):
        """Test Struct equality comparison."""
        s1 = Types.Struct({"x": Types.Int(), "y": Types.Float()})
        s2 = Types.Struct({"x": Types.Int(), "y": Types.Float()})
        s3 = Types.Struct({"x": Types.Int()})
        
        assert s1 == s2
        assert s1 != s3

    def test_struct_empty(self):
        """Test Struct with no fields."""
        struct_type = Types.Struct({})
        arrow_type = struct_type.to_arrow()
        assert pa.types.is_struct(arrow_type)
        assert len(arrow_type) == 0


class TestListType:
    """Tests for List type (arrays)."""

    def test_list_of_strings(self):
        """Test List of strings."""
        list_type = Types.List(Types.String())
        arrow_type = list_type.to_arrow()
        assert pa.types.is_list(arrow_type)
        assert arrow_type.value_type == pa.string()

    def test_list_of_ints(self):
        """Test List of integers."""
        list_type = Types.List(Types.Int())
        arrow_type = list_type.to_arrow()
        assert pa.types.is_list(arrow_type)
        assert arrow_type.value_type == pa.int64()

    def test_list_of_structs(self):
        """Test List of Struct (array of objects)."""
        list_type = Types.List(
            Types.Struct({
                "id": Types.Int(),
                "name": Types.String()
            })
        )
        arrow_type = list_type.to_arrow()
        assert pa.types.is_list(arrow_type)
        assert pa.types.is_struct(arrow_type.value_type)

    def test_list_of_lists(self):
        """Test nested List (2D array)."""
        list_type = Types.List(Types.List(Types.Float()))
        arrow_type = list_type.to_arrow()
        assert pa.types.is_list(arrow_type)
        assert pa.types.is_list(arrow_type.value_type)
        assert arrow_type.value_type.value_type == pa.float64()

    def test_list_equality(self):
        """Test List equality comparison."""
        l1 = Types.List(Types.String())
        l2 = Types.List(Types.String())
        l3 = Types.List(Types.Int())
        
        assert l1 == l2
        assert l1 != l3


class TestAnyType:
    """Tests for Any type (polymorphic fields)."""

    def test_any_to_arrow(self):
        """Test Any type Arrow conversion to Struct."""
        any_type = Types.Any()
        arrow_type = any_type.to_arrow()
        
        assert pa.types.is_struct(arrow_type)
        # Should have fields for different value types
        field_names = [arrow_type.field(i).name for i in range(len(arrow_type))]
        assert "float_value" in field_names
        assert "int64_value" in field_names
        assert "string_value" in field_names
        assert "bool_value" in field_names
        assert "null_value" in field_names

    def test_any_equality(self):
        """Test Any equality (all Any instances are equal)."""
        a1 = Types.Any()
        a2 = Types.Any()
        assert a1 == a2

    def test_any_repr(self):
        """Test Any string representation."""
        any_type = Types.Any()
        assert repr(any_type) == "Any()"


class TestTypeHashing:
    """Tests for type hashability (for use in dicts/sets)."""

    def test_primitive_types_hashable(self):
        """Test that primitive types can be used as dict keys."""
        type_dict = {
            Types.String(): "string",
            Types.Int(): "int",
            Types.Float(): "float",
            Types.Bool(): "bool"
        }
        assert type_dict[Types.String()] == "string"
        assert type_dict[Types.Int()] == "int"

    def test_timestamp_hashable(self):
        """Test that Timestamp can be used as dict keys."""
        type_dict = {
            Types.Timestamp("ms", tz="UTC"): "ms_utc",
            Types.Timestamp("ns", tz="UTC"): "ns_utc"
        }
        assert type_dict[Types.Timestamp("ms", tz="UTC")] == "ms_utc"

    def test_types_in_set(self):
        """Test that types can be added to sets."""
        type_set = {Types.String(), Types.Int(), Types.Float()}
        assert len(type_set) == 3
        assert Types.String() in type_set


class TestTypeInheritance:
    """Tests for BaseType abstract class."""

    def test_all_types_are_base_type(self):
        """Test that all types inherit from BaseType."""
        types_to_test = [
            Types.String(),
            Types.Int(),
            Types.Float(),
            Types.Bool(),
            Types.Timestamp(),
            Types.ObjectId(),
            Types.Any(),
            Types.Struct({}),
            Types.List(Types.String())
        ]
        
        for t in types_to_test:
            assert isinstance(t, Types.BaseType)

    def test_base_type_has_to_arrow(self):
        """Test that all types implement to_arrow()."""
        types_to_test = [
            Types.String(),
            Types.Int(),
            Types.Float(),
            Types.Bool(),
            Types.Timestamp(),
            Types.ObjectId(),
            Types.Any(),
            Types.Struct({}),
            Types.List(Types.String())
        ]
        
        for t in types_to_test:
            assert hasattr(t, 'to_arrow')
            assert callable(t.to_arrow)
            # Should return PyArrow DataType
            arrow_type = t.to_arrow()
            assert isinstance(arrow_type, pa.DataType)
