from nifiapi.__jvm__ import JvmHolder

class FlowFileTransformResult:
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransformResult']

    def __init__(self, relationship, attributes = None, contents = None):
        self.relationship = relationship
        self.attributes = attributes
        self.contents = contents

    def getRelationship(self):
        return self.relationship

    def getContents(self):
        return self.contents

    def getAttributes(self):
        if self.attributes is None:
            return None

        map = JvmHolder.jvm.java.util.HashMap()
        for key, value in self.attributes.items():
            map.put(key, value)

        return map