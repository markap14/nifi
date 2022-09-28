import ProcessorUtil
from nifiapi.flowfiletransform import FlowFileTransformResult

class WriteNumber:
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm):
        pass

    def transform(self, context, flowFile):
        util = ProcessorUtil.ProcessorUtil()
        num = util.generate_random_number()
        return FlowFileTransformResult(relationship = "success", contents = str.encode(str(num)))