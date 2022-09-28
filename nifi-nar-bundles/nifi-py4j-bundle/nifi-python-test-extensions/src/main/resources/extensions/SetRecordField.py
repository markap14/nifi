from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.properties import ExpressionLanguageScope
from nifiapi.recordtransform import RecordTransformResult
from nifiapi.recordtransform import RecordCodec


class SetRecordField:
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'


    def __init__(self, jvm):
        pass

    def transform(self, context, record, attributemap):
        # Convert Record to a Python dictionary
        rec = RecordCodec().record_to_dict(record)

        # Update dictionary based on the dynamic properties provided by user
        attributes = attributemap.getAttributes()
        for key in context.getProperties().keySet():
            if not key.isDynamic():
                continue

            propname = key.getName()
            rec[propname] = context.getProperty(propname).evaluateAttributeExpressions(attributes).getValue()

        # Determine the partition
        if 'group' in rec:
            partition = {'group': rec['group']}
        else:
            partition = None

        # Return the result
        return RecordTransformResult(record=rec, relationship ='success', partition=partition)


    def getDynamicPropertyDescriptor(self, name):
        return PropertyDescriptor(
            name=name,
            description="Specifies the value to set for the '" + name + "' field",
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators = [StandardValidators.ALWAYS_VALID]
        )
