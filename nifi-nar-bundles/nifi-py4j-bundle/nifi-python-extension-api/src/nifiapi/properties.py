from enum import Enum
from nifiapi.__jvm__ import JvmHolder

class ExpressionLanguageScope(Enum):
    NONE = 1
    VARIABLE_REGISTRY = 2
    FLOWFILE_ATTRIBUTES = 3


class StandardValidators:
    __standard_validators__ = JvmHolder.jvm.org.apache.nifi.processor.util.StandardValidators

    ALWAYS_VALID = JvmHolder.jvm.org.apache.nifi.components.Validator.VALID
    NON_EMPTY_VALIDATOR = __standard_validators__.NON_EMPTY_VALIDATOR
    INTEGER_VALIDATOR = __standard_validators__.INTEGER_VALIDATOR
    POSITIVE_INTEGER_VALIDATOR = __standard_validators__.POSITIVE_INTEGER_VALIDATOR
    POSITIVE_LONG_VALIDATOR = __standard_validators__.POSITIVE_LONG_VALIDATOR
    NON_NEGATIVE_INTEGER_VALIDATOR = __standard_validators__.NON_NEGATIVE_INTEGER_VALIDATOR
    NUMBER_VALIDATOR = __standard_validators__.NUMBER_VALIDATOR
    LONG_VALIDATOR = __standard_validators__.LONG_VALIDATOR
    PORT_VALIDATOR = __standard_validators__.PORT_VALIDATOR
    NON_EMPTY_EL_VALIDATOR = __standard_validators__.NON_EMPTY_EL_VALIDATOR
    HOSTNAME_PORT_LIST_VALIDATOR = __standard_validators__.HOSTNAME_PORT_LIST_VALIDATOR
    BOOLEAN_VALIDATOR = __standard_validators__.BOOLEAN_VALIDATOR
    URL_VALIDATOR = __standard_validators__.URL_VALIDATOR
    URI_VALIDATOR = __standard_validators__.URI_VALIDATOR
    REGULAR_EXPRESSION_VALIDATOR = __standard_validators__.REGULAR_EXPRESSION_VALIDATOR
    REGULAR_EXPRESSION_WITH_EL_VALIDATOR = __standard_validators__.REGULAR_EXPRESSION_WITH_EL_VALIDATOR
    TIME_PERIOD_VALIDATOR = __standard_validators__.TIME_PERIOD_VALIDATOR
    DATA_SIZE_VALIDATOR = __standard_validators__.DATA_SIZE_VALIDATOR
    FILE_EXISTS_VALIDATOR = __standard_validators__.FILE_EXISTS_VALIDATOR



class PropertyDependency:
    def __init__(self, property_descriptor, dependent_values=None):
        if dependent_values is None:
            dependent_values = []

        if isinstance(property_descriptor, str):
            self.property_name = property_descriptor
        else:
            self.property_name = property_descriptor.getName()
        self.dependentValues = dependent_values

    @staticmethod
    def from_java_dependency(java_dependencies):
        if java_dependencies is None or len(java_dependencies) == 0:
            return None

        dependencies = []
        for dependency in java_dependencies:
            dependencies.append(PropertyDependency(dependency.getPropertyName(), dependency.getDependentValues()))

        return dependencies


class ResourceDefinition:
    def __init__(self, allow_multiple=False, allow_file=True, allow_url=False, allow_directory=False, allow_text=False):
        self.allow_multiple = allow_multiple
        self.allow_file = allow_file
        self.allow_url = allow_url
        self.allow_directory = allow_directory
        self.allow_text = allow_text

    @staticmethod
    def from_java_definition(java_definition):
        if java_definition is None:
            return None

        allow_multiple = java_definition.getCardinality().name() == "MULTIPLE"
        resource_types = java_definition.getResourceTypes()
        allow_file = False
        allow_url = False
        allow_directory = False
        allow_text = False
        for type in resource_types:
            name = type.name()
            if name == "FILE":
                allow_file = True
            elif name == "DIRECTORY":
                allow_directory = True
            elif name == "TEXT":
                allow_text = True
            elif name == "URL":
                allow_url = True

        return ResourceDefinition(allow_multiple, allow_file, allow_url, allow_directory, allow_text)


class PropertyDescriptor:
    def __init__(self, name, description, required=False, sensitive=False,
                 display_name=None, default_value=None, allowable_values=None,
                 dependencies=None, expression_language_scope=ExpressionLanguageScope.NONE,
                 dynamic=False, validators=None,
                 resource_definition=None, controller_service_definition=None):
        if validators is None:
            validators = []

        self.name = name
        self.description = description
        self.required = required
        self.sensitive = sensitive
        self.displayName = display_name
        self.defaultValue = default_value
        self.allowableValues = allowable_values
        self.dependencies = dependencies
        self.expressionLanguageScope = expression_language_scope
        self.dynamic = dynamic
        self.validators = validators
        self.resourceDefinition = resource_definition
        self.controllerServiceDefinition = controller_service_definition

    @staticmethod
    def from_java_descriptor(java_descriptor):
        # Build the dependencies
        dependencies = PropertyDependency.from_java_dependency(java_descriptor.getDependencies())

        # Build the allowable values
        allowable = java_descriptor.getAllowableValues()
        if allowable is None or len(allowable) == 0:
            allowable_values = None
        else:
            allowable_values = []
            for value in allowable:
                allowable_values.append(value.getValue())

        # Build the resource definition
        resource_definition = ResourceDefinition.from_java_definition(java_descriptor.getResourceDefinition())

        el_scope = java_descriptor.getExpressionLanguageScope()
        el_scope_name = None if el_scope is None else el_scope.name()

        return PropertyDescriptor(java_descriptor.getName(), java_descriptor.getDescription(),
                                  required = java_descriptor.isRequired(),
                                  sensitive = java_descriptor.isSensitive(),
                                  display_name = java_descriptor.getDisplayName(),
                                  default_value = java_descriptor.getDefaultValue(),
                                  allowable_values = allowable_values,
                                  expression_language_scope = el_scope_name,
                                  dynamic = java_descriptor.isDynamic(),
                                  validators = java_descriptor.getValidators(),
                                  controller_service_definition = java_descriptor.getControllerServiceDefinition(),
                                  dependencies = dependencies,
                                  resource_definition = resource_definition)


    def to_java_descriptor(self, gateway, cs_type_lookup):
        # TODO: Consider dependencies
        el_scope = gateway.jvm.org.apache.nifi.expression.ExpressionLanguageScope.valueOf(self.expressionLanguageScope.name)

        builder = gateway.jvm.org.apache.nifi.components.PropertyDescriptor.Builder() \
            .name(self.name) \
            .displayName(self.displayName) \
            .description(self.description) \
            .required(self.required) \
            .sensitive(self.sensitive) \
            .defaultValue(self.defaultValue) \
            .expressionLanguageSupported(el_scope) \
            .dynamic(self.dynamic)

        if self.resourceDefinition is not None:
            self.__add_resource_definition__(gateway, self.resourceDefinition, builder)

        if self.controllerServiceDefinition is not None:
            cs_type = cs_type_lookup.lookup(self.controllerServiceDefinition)
            builder.identifiesControllerService(cs_type)

        if self.allowableValues is not None:
            builder.allowableValues(self.__get_allowable_values__(gateway))

        for validator in self.validators:
            builder.addValidator(validator)

        return builder.build()


    def __get_allowable_values__(self, gateway):
        if self.allowableValues is None:
            return None
        values = gateway.jvm.java.util.ArrayList()
        for value in self.allowableValues:
            values.add(value)

        return values

    def __add_resource_definition__(self, gateway, resouce_definition, builder):
        allowed_types = 0
        if resouce_definition.allow_file:
            allowed_types += 1
        if resouce_definition.allow_directory:
            allowed_types += 1
        if resouce_definition.allow_url:
            allowed_types += 1
        if resouce_definition.allow_text:
            allowed_types += 1

        array_type = gateway.jvm.org.apache.nifi.components.resource.ResourceType
        types = gateway.new_array(array_type, allowed_types)
        index = 0
        if resouce_definition.allow_file:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.FILE
            index += 1
        if resouce_definition.allow_directory:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.DIRECTORY
            index += 1
        if resouce_definition.allow_url:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.URL
            index += 1
        if resouce_definition.allow_text:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.TEXT
            index += 1

        cardinality = gateway.jvm.org.apache.nifi.components.resource.ResourceCardinality.MULTIPLE if resouce_definition.allow_multiple else \
            gateway.jvm.org.apache.nifi.components.resource.ResourceCardinality.SINGLE

        builder.identifiesExternalResource(cardinality, types[0], types[1:])


