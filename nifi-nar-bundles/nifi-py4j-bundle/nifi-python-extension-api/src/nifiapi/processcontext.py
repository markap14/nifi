from enum import Enum
from nifiapi.__jvm__ import JvmHolder
from nifiapi.properties import PropertyDescriptor
import re


# TODO: Eventually move to properties.py
class ProcessContext:
    __trivial_attribute_reference__ = re.compile("\$\{([^${}\[\],:;/*\' \t\r\n\d][^${}\[\],:;/*\' \t\r\n]*)}")
    __escaped_attribute_reference__ = re.compile("\$\{'([^${}\[\],:;/*\' \t\r\n\d][^${}\[\],:;/*\' \t\r\n]*)'}")

    def __init__(self, java_context):
        self.java_context = java_context

        descriptors = java_context.getProperties().keySet()
        self.name = java_context.getName()
        self.property_values = {}
        self.descriptor_value_map = {}

        for descriptor in descriptors:
            property_value = java_context.getProperty(descriptor.getName())
            string_value = property_value.getValue()

            property_value = self.__create_python_property_value__(descriptor.isExpressionLanguageSupported(), property_value, string_value)
            self.property_values[descriptor.getName()] = property_value

            python_descriptor = PropertyDescriptor.from_java_descriptor(descriptor)
            self.descriptor_value_map[python_descriptor] = string_value


    def __create_python_property_value__(self, el_supported, java_property_value, string_value):
        el_present = java_property_value.isExpressionLanguagePresent()
        referenced_attribute = None
        if el_present:
            trivial_match = self.__trivial_attribute_reference__.match(string_value)
            if trivial_match is not None:
                referenced_attribute = trivial_match.group(1)
            else:
                escaped_match = self.__escaped_attribute_reference__.match(string_value)
                if escaped_match is not None:
                    referenced_attribute = escaped_match.group(1)

        return PythonPropertyValue(java_property_value, string_value, el_supported, el_present, referenced_attribute)


    def getProperty(self, descriptor):
        property_name = descriptor if isinstance(descriptor, str) else descriptor.name
        return self.property_values[property_name]

    def getProperties(self):
        return self.descriptor_value_map

    def newPropertyValue(self, value):
        java_property_value = self.java_context.newPropertyValue(value)
        return self.__create_python_property_value__(True, java_property_value, value)

    def getName(self):
        return self.name



class TimeUnit(Enum):
    NANOSECONDS = "NANOSECONDS",
    MICROSECONDS = "MICROSECONDS",
    MILLISECONDS = "MILLISECONDS",
    SECONDS = "SECONDS",
    MINUTES = "MINUTES",
    HOURS = "HOURS",
    DAYS = "DAYS"


class DataUnit(Enum):
    B = "B",
    KB = "KB",
    MB = "MB",
    GB = "GB",
    TB = "TB"


class PythonPropertyValue:
    def __init__(self, property_value, string_value, el_supported, el_present, referenced_attribute):
        self.value = string_value
        self.property_value = property_value
        self.el_supported = el_supported
        self.el_present = el_present
        self.referenced_attribute = referenced_attribute

    def getValue(self):
        return self.value

    def isSet(self):
        return self.value is not None

    def isExpressionLanguagePresent(self):
        return self.el_present

    def asInteger(self):
        if self.value is None:
            return None
        return int(self.value)

    def asBoolean(self):
        if self.value is None:
            return None
        return bool(self.value)

    def asFloat(self):
        if self.value is None:
            return None
        return float(self.value)

    def asTimePeriod(self, time_unit):
        javaTimeUnit = JvmHolder.jvm.java.util.concurrent.TimeUnit.valueOf(time_unit._name_)
        return self.property_value.asTimePeriod(javaTimeUnit)

    def asDataSize(self, data_unit):
        javaDataUnit = JvmHolder.jvm.org.apache.nifi.processor.DataUnit.valueOf(data_unit._name_)
        return self.property_value.asDataSize(javaDataUnit)

    def asControllerService(self):
        return self.property_value.asControllerService()

    def asResource(self):
        return self.property_value.asResource()

    def asResources(self):
        return self.property_value.asResources()

    def evaluateAttributeExpressions(self, attributeMap=None):
        # If Expression Language is supported and present, evaluate it and return a new PropertyValue.
        # Otherwise just return self, in order to avoid the cost of making the call to Java for evaluateAttributeExpressions
        new_property_value = None
        if self.el_supported and self.el_present:
            if self.referenced_attribute is not None:
                attribute_value = attributeMap.getAttribute(self.referenced_attribute)
                new_property_value = self.property_value
                if attribute_value is None:
                    new_string_value = ""
                else:
                    new_string_value = attribute_value
            else:
                # TODO: Consider having property_value wrapped in another class that delegates to the existing property_value but allows evaluateAttributeExpressions to take in an AttributeMap.
                #       This way we can avoid the call to getAttributes() here, which is quite expensive
                new_property_value = self.property_value.evaluateAttributeExpressions(attributeMap.getAttributes())
                new_string_value = new_property_value.getValue()

            return PythonPropertyValue(new_property_value, new_string_value, self.el_supported, False, None)

        return self
