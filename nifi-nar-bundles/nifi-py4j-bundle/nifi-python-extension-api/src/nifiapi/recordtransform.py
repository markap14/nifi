import json
from abc import ABC, abstractmethod


class RecordTransform(ABC):
    def __init__(self, **kwargs):
        pass

    def setContext(self, context):
        # TODO: Create a python-side context and set it
        pass

    def transformRecord(self, context, recordjson, schema, attributemap):
        parsed = json.loads(recordjson)
        result = self.transform(context, parsed, schema, attributemap)
        result_record = result.getRecord()
        resultjson = None if result_record is None else json.dumps(result_record)
        return __RecordTransformResult__(result, resultjson)

    @abstractmethod
    def transform(self, context, record, schema, attributemap):
        pass



class __RecordTransformResult__:
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransformResult']

    def __init__(self, processor_result, recordJson):
        self.processor_result = processor_result
        self.recordJson = recordJson

    def getRecordJson(self):
        return self.recordJson

    def getSchema(self):
        return self.processor_result.schema

    def getRelationship(self):
        return self.processor_result.relationship

    def getPartition(self):
        return self.processor_result.partition



class RecordTransformResult:

    def __init__(self, record=None, schema=None, relationship="success", partition=None):
        self.record = record
        self.schema = schema
        self.relationship = relationship
        self.partition = partition

    def getRecord(self):
        return self.record

    def getSchema(self):
        return self.schema

    def getRelationship(self):
        return self.relationship

    def getPartition(self):
        return self.partition
