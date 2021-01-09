import io
import json
import avro.schema
from avro.io import BinaryDecoder, DatumReader, DatumWriter, BinaryEncoder
from pyspark import SparkContext, SparkConf

schema_str = "{\"namespace\":\"com.ey.cust\",\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"consumerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"customerName\",\"type\":\"string\"},{\"name\":\"customerZip\",\"type\":\"string\"}]}"
payload_str = "{\"consumerId\": \"A00AD3\", \"customerName\": \"Charlie\", \"customerZip\": \"fdsfd\"}"


def deserialize_avro(binary_data, schema):
    """
    Function used to deserialize an avro binary data
    :param schema: avro schema of binary data
    :param binary_data: event data in binary encoded (bytes)
    :return: deserialized data and corresponding schema
    """
    bytes_reader = io.BytesIO(binary_data)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    data = reader.read(decoder)
    return data, schema


def serialize_avro(payload_str, schema):
    """
    Function used to serialize a json event to binary format based on avro schema
    :param schema: avro schema of payload
    :param payload_str: event data in json string format
    :return: avro serialized binary data and corresponding schema
    """
    payload_json = json.loads(payload_str)
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(payload_json, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes, schema


avro_schema = avro.schema.parse(schema_str)
conf = SparkConf()
conf.setAppName('test_pyspark')
conf.set("spark.driver.host", "127.0.0.1")
sc = SparkContext(conf=conf)
arr = (sc.parallelize([(payload_str, avro_schema)])
       .map(lambda payload_schema_pair: serialize_avro(payload_schema_pair[0], payload_schema_pair[1]))
       .map(lambda bytes_schema_pair: deserialize_avro(bytes_schema_pair[0], bytes_schema_pair[1]))
       .collect())

print(arr)

