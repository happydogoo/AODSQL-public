import pytest
from src.engine.storage.tuple_serializer import TupleSerializer

class DummyCol:
    def __init__(self, name, dtype):
        self.column_name = name
        self.data_type = dtype

def test_int_serialization():
    schema = [DummyCol('id', 'INT')]
    ser = TupleSerializer(schema)
    row = (123,)
    data = ser.serialize(row)
    assert isinstance(data, bytes)
    row2 = ser.deserialize(data)
    assert row2 == row

def test_varchar_serialization():
    schema = [DummyCol('name', 'VARCHAR(10)')]
    ser = TupleSerializer(schema)
    row = ('Alice',)
    data = ser.serialize(row)
    row2 = ser.deserialize(data)
    assert row2[0].startswith('Alice')

def test_decimal_serialization():
    schema = [DummyCol('score', 'DECIMAL(6,2)')]
    ser = TupleSerializer(schema)
    row = (3.14,)
    data = ser.serialize(row)
    row2 = ser.deserialize(data)
    assert abs(float(row2[0]) - 3.14) < 1e-2

def test_mixed_schema():
    schema = [DummyCol('id', 'INT'), DummyCol('name', 'VARCHAR(8)'), DummyCol('score', 'FLOAT')]
    ser = TupleSerializer(schema)
    row = (1, 'Bob', 4.56)
    data = ser.serialize(row)
    row2 = ser.deserialize(data)
    assert row2[0] == 1
    assert row2[1].startswith('Bob')
    assert abs(float(row2[2]) - 4.56) < 1e-2

def test_none_and_default():
    schema = [DummyCol('id', 'INT'), DummyCol('desc', 'VARCHAR(10)')]
    ser = TupleSerializer(schema)
    row = (42, None)
    data = ser.serialize(row)
    row2 = ser.deserialize(data)
    assert row2[0] == 42
    assert isinstance(row2[1], str) 