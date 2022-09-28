from nifiapi.__jvm__ import JvmHolder
from datetime import datetime
from collections import OrderedDict

class RecordTransformResult:
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransformResult']

    def __init__(self, record=None, schema=None, relationship="success", partition=None):
        self.record = record
        self.schema = schema
        self.relationship = relationship
        self.partition = partition

    def getRecord(self):
        values = self.__to_hashmap__(self.record)
        return values

    def getSchema(self):
        return self.schema

    def getRelationship(self):
        return self.relationship

    def getPartition(self):
        return self.__to_hashmap__(self.partition)

    def __to_hashmap__(self, dict):
        if dict is None:
            return None

        hashmap = JvmHolder.jvm.java.util.HashMap()
        for key, value in dict.items():
            hashmap.put(key, value)
        return hashmap


class RecordCodec:
    """
    A utility class for translating between Java Record objects and Plain Old Python Objects,
    such as dicts and lists
    """

    def dict_to_map(self, dict):
        """
        Creates a Java Map<String, Object> that reflects the given dict.
        If a value in the dict is a list will be converted into a Java ArrayList,
        and a timestamp will be converted into a java.util.Date object, etc.

        :param dict: the dict to convert
        :return: a Java Map<String, Object> representing the given dict
        """
        values = JvmHolder.jvm.java.util.HashMap()
        for key, value in dict.items():
            values.put(key, self.__to_record_object__(value))
        return values


    def record_to_dict(self, record):
        """
        Creates a dict representing the values of the given Record. This is done recursively, so that
        if the Record contains embedded records, those are also converted into dicts, and so on.

        :param record: the Record to convert
        :return: a Pythonic dict representation of the given record
        """

        dict = OrderedDict()
        field_names = record.getRawFieldNames()
        for name in field_names:
            value = record.getValue(name)
            popo = self.__to_popo__(value)
            dict[name] = popo

        return dict


    def __to_record_object__(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return value
        if isinstance(value, bool):
            return value
        if isinstance(value, dict):
            return self.dict_to_map(value)
        if isinstance(value, list):
            array_list = JvmHolder.jvm.java.util.ArrayList()
            for val in value:
                array_list.add(val)
            return array_list
        if isinstance(value, datetime):
            return JvmHolder.jvm.java.util.Date(value.timestamp())
        return None


    def __to_popo__(self, value):
        """
        Converts a Record-specific value to a Plain Old Python Object
        :param value: the value to convert
        :return: the Python-friendly object
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return value
        if isinstance(value, bool):
            return value

        if 'getRawFieldNames' in dir(value):
            return self.record_to_dict(value)

        class_name = value.getClass().getName()
        if class_name == "java.util.Date" or class_name == "java.sql.Date":
            return datetime.timestamp(value.getTime())

        if value.getClass().isArray():
            array = []
            for val in value:
                converted = self.__to_popo__(val)
                array.append(converted)
            return array
