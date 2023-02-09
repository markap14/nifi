class ProcessContext:

    def __init__(self, java_context):
      descriptors = java_context.getProperties().keySet()
      self.property_values = {}
      for descriptor in descriptors:
          if descriptor.isExpressionLanguageSupported():
              dosomething()
          else:
              propertyvalue = java_context.getProperty(descriptor.getName()).getValue()
              self.property_values[descriptor.getName()] = propertyvalue


class SimplePropertyValue:
    def __init__(self, value):
        self.value = value

    def getValue(self):
        return self.value