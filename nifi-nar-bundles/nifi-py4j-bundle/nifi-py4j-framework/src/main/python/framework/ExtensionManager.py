import os
import importlib
import sys
import importlib.util  # Note requires Python 3.4+
import inspect
import logging
import subprocess
import ast
import pkgutil
from pathlib import Path

logger = logging.getLogger("org.apache.nifi.py4j.ExtensionManager")

NIFI_API_DEPENDENCY = "nifiapi"

# A simple wrapper class to encompass a processor type and its version
class ExtensionId:
    def __init__(self, classname=None, version=None):
        self.classname = classname
        self.version = version

    def __hash__(self):
        return hash((self.classname, self.version))

    def __eq__(self, other):
        return (self.classname, self.version) == (other.classname, other.version)


class ExtensionDetails:
    class Java:
        implements = ['org.apache.nifi.python.PythonProcessorDetails']

    def __init__(self, gateway, type, version='Unknown', dependencies=None, source_location=None, package_name=None, description=None, tags=None):
        self.gateway = gateway
        if dependencies is None:
            dependencies = []
        if tags is None:
            tags = []

        self.type = type
        self.version = version
        self.dependencies = dependencies
        self.source_location = source_location
        self.package_name = package_name
        self.description = description
        self.tags = tags

    def getProcessorType(self):
        return self.type

    def getProcessorVersion(self):
        return self.version

    def getSourceLocation(self):
        return self.source_location

    def getPyPiPackageName(self):
        return self.package_name

    def getDependencies(self):
        list = self.gateway.jvm.java.util.ArrayList()
        for dep in self.dependencies:
            list.add(dep)

        return list

    def getCapabilityDescription(self):
        return self.description

    def getTags(self):
        list = self.gateway.jvm.java.util.ArrayList()
        for tag in self.tags:
            list.add(tag)

        return list




class ExtensionManager:
    """
    ExtensionManager is responsible for discovery of extensions types and the lifecycle management of those extension types.
    Discovery of extension types includes finding what extension types are available
    (e.g., which Processor types exist on the system), as well as information about those extension types, such as
    the extension's documentation (tags and capability description).

    Lifecycle management includes determining the third-party dependencies that an extension has and ensuring that those
    third-party dependencies have been imported.
    """

    processorInterfaces = ['org.apache.nifi.python.processor.FlowFileTransform', 'org.apache.nifi.python.processor.RecordTransform']
    processor_details = {}
    processor_class_by_name = {}
    module_files_by_extension_type = {}
    dependency_directories = {}

    def __init__(self, gateway):
        self.gateway = gateway


    def getProcessorTypes(self):
        """
        :return: a list of Processor types that have been discovered by the #discoverExtensions method
        """
        return self.processor_details.values()

    def getProcessorClass(self, type, version, work_dir):
        """
        Returns the Python class that can be used to instantiate a processor of the given type.
        Additionally, it ensures that the required third-party dependencies are on the system path in order to ensure that
        the necessary libraries are available to the Processor so that it can be instantiated and used.

        :param type: the type of Processor
        :param version: the version of the Processor
        :param work_dir: the working directory for extensions
        :return: the Python class that can be used to instantiate a Processor of the given type and version

        :raises ValueError: if there is no known Processor with the given type and version
        """
        id = ExtensionId(classname=type, version=version)
        if id in self.processor_class_by_name:
            return self.processor_class_by_name[id]

        if id not in self.module_files_by_extension_type:
            raise ValueError('Invalid Processor Type: No module is known to contain Processor of type ' + type + ' version ' + version)
        module_file = self.module_files_by_extension_type[id]

        if id in self.processor_details:
            extension_working_dir = os.path.join(work_dir, 'extensions', type, version)
            sys.path.insert(0, extension_working_dir)

        details = self.processor_details[id]
        processor_class = self.__load_extension_module__(module_file, details.local_dependencies)
        self.processor_class_by_name[id] = processor_class
        return processor_class


    def reload_processor(self, processor_type, version, work_dir):
        """
        Reloads the class definition for the given processor type. This is used in order to ensure that any changes that have
        been made to the Processor are reloaded and will take effect.

        :param processor_type: the type of the processor whose class definition should be reloaded
        :param version: the version of the processor whose class definition should be reloaded
        :param work_dir: the working directory
        :return: the new class definition
        """
        id = ExtensionId(classname=processor_type, version=version)

        # get the python module file that contains the specified processor
        module_file = self.get_module_file(processor_type, version)

        # Call load_extension to ensure that we load all necessary dependencies, in case they have changed
        self.__gather_extension_details__(module_file, work_dir)

        # Reload the processor class itself
        details = self.processor_details[id]
        processor_class = self.__load_extension_module__(module_file, details.local_dependencies)

        # Update our cache so that when the processor is created again, the new class will be used
        self.processor_class_by_name[id] = processor_class


    def get_module_file(self, processor_type, version):
        """
        Returns the module file that contains the source for the given Processor type and version
        :param processor_type: the Processor type
        :param version: the version of the Processor
        :return: the file that contains the source for the given Processor

        :raises ValueError: if no Processor type is known for the given type and version
        """
        id = ExtensionId(processor_type, version)
        if id not in self.module_files_by_extension_type:
            raise ValueError('Invalid Processor Type: No module is known to contain Processor of type ' + processor_type + ' version ' + version)

        return self.module_files_by_extension_type[id]


    # Discover extensions using the 'prefix' method described in
    # https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/
    def discoverExtensions(self, dirs, work_dir):
        """
        Discovers any extensions that are available in any of the given directories, as well as any extensions that are available
        from PyPi repositories

        :param dirs: the directories to check for any local extensions
        :param work_dir: the working directory
        """
        self.__discover_local_extensions__(dirs, work_dir)
        self.__discover_extensions_from_pypi__(work_dir)

    def __discover_extensions_from_pypi__(self, work_dir):
        self.__discover_extensions_from_paths__(None, work_dir, True)

    def __discover_local_extensions__(self, dirs, work_dir):
        self.__discover_extensions_from_paths__(dirs, work_dir, False)


    def __discover_extensions_from_paths__(self, paths, work_dir, require_nifi_prefix):
        for finder, name, ispkg in pkgutil.iter_modules(paths):
            if not require_nifi_prefix or name.startswith('nifi_'):
                module_file = '<Unknown Module File>'
                try:
                    module = finder.find_module(name)
                    module_file = module.path
                    logger.info('Discovered extension %s' % module_file)

                    self.__gather_extension_details__(module_file, work_dir)
                except Exception:
                    logger.error("Failed to load Python extensions from module file {0}. This module will be ignored.".format(module_file), exc_info=True)



    def __gather_extension_details__(self, module_file, work_dir, local_dependencies=None):
        path = Path(module_file)
        basename = os.path.basename(module_file)

        # If no local_dependencies have been provided, check to see if there are any.
        # We consider any Python module in the same directory as a local dependency
        if local_dependencies is None:
            local_dependencies = []

            if basename == '__init__.py':
                dir = path.parent
                for filename in os.listdir(dir):
                    if not filename.endswith('.py'):
                        continue
                    if filename == '__init__.py':
                        continue

                    child_module_file = os.path.join(dir, filename)
                    local_dependencies.append(child_module_file)

        # If the module file is an __init__.py file, we check all module files in the same directory.
        if basename == '__init__.py':
            dir = path.parent
            for filename in os.listdir(dir):
                if not filename.endswith('.py'):
                    continue
                if filename == '__init__.py':
                    continue

                child_module_file = os.path.join(dir, filename)
                self.__gather_extension_details__(child_module_file, work_dir, local_dependencies=local_dependencies)

        classes_and_details = self.__get_processor_classes_and_details__(module_file)
        for classname, details in classes_and_details.items():
            id = ExtensionId(classname, details.version)
            logger.info("Found local dependencies {0} for {1}".format(local_dependencies, classname))

            details.local_dependencies = local_dependencies

            # Add class name to processor types only if not there already
            if id not in self.processor_details:
                self.processor_details[id] = details

            self.module_files_by_extension_type[id] = module_file

            # If there are any dependencies, use pip to install them
            extension_working_dir = os.path.join(work_dir, 'extensions', classname, details.version)
            self.__import_external_dependencies__(classname, details.dependencies, extension_working_dir)


    def __get_dependencies_for_extension_type__(self, extension_type, version):
        id = ExtensionId(extension_type, version)
        return self.processor_details[id].dependencies

    def __get_processor_classes_and_details__(self, module_file):
        # Parse the python file
        with open(module_file) as file:
            root_node = ast.parse(file.read())

        details_by_class = {}

        # Get top-level class nodes (e.g., MyProcessor)
        class_nodes = self.__get_class_nodes__(root_node)

        for class_node in class_nodes:
            logger.debug("Checking if class %s is a processor" % class_node.name)
            if self.__is_processor_class_node__(class_node):
                logger.info("Discovered Processor class {0} in module {1}".format(class_node.name, module_file))
                details = self.__get_processor_details__(class_node, module_file)
                details_by_class[class_node.name] = details

        return details_by_class


    def __is_processor_class_node__(self, class_node):
        """
        Checks if the Abstract Syntax Tree (AST) Node represents a Processor class.
        We are looking for any classes within the given module file that look like:

        class MyProcessor:
            ...
            class Java:
                implements = ['org.apache.nifi.python.processor.FlowFileTransform']

        :param class_node: the abstract syntax tree (AST) node
        :return: True if the AST Node represents a Python Class that is a Processor, False otherwise
        """

        # Look for a 'Java' sub-class
        child_class_nodes = self.__get_class_nodes__(class_node)

        for child_class_node in child_class_nodes:
            if child_class_node.name == 'Java':
                # Look for an assignment that assigns values to the `implements` keyword
                assignment_nodes = self.__get_assignment_nodes__(child_class_node)
                for assignment_node in assignment_nodes:
                    if (len(assignment_node.targets) == 1 and assignment_node.targets[0].id == 'implements'):
                        assigned_values = assignment_node.value.elts
                        for assigned_value in assigned_values:
                            if assigned_value.value in self.processorInterfaces:
                                return True
        return False


    def __get_processor_details__(self, class_node, module_file):
        # Look for a 'ProcessorDetails' class
        child_class_nodes = self.__get_class_nodes__(class_node)

        for child_class_node in child_class_nodes:
            if child_class_node.name == 'ProcessorDetails':
                logger.debug('Found ProcessorDetails class in %s' % class_node.name)
                version = self.__get_processor_version__(child_class_node, class_node.name)
                dependencies = self.__get_processor_dependencies__(child_class_node, class_node.name)
                description = self.__get_processor_description__(child_class_node, class_node.name)
                tags = self.__get_processor_tags__(child_class_node, class_node.name)

                return ExtensionDetails(gateway=self.gateway,
                                        type=class_node.name,
                                        version=version,
                                        dependencies=dependencies,
                                        source_location=module_file,
                                        description=description,
                                        tags=tags)

        return ExtensionDetails(gateway=self.gateway,
                                type=class_node.name,
                                version='Unknown',
                                dependencies=[],
                                source_location=module_file)


    def __get_processor_version__(self, details_node, class_name):
        assignment_nodes = self.__get_assignment_nodes__(details_node)
        for assignment_node in assignment_nodes:
            if (len(assignment_node.targets) == 1 and assignment_node.targets[0].id == 'version'):
                assigned_values = assignment_node.value.value
                logger.info("Found version of {0} to be {1}".format(class_name, assigned_values))
                return assigned_values

        # No dependencies found
        logger.info("Found no version information for {0}".format(class_name))
        return 'Unknown'


    def __get_processor_dependencies__(self, details_node, class_name):
        deps = self.__get_assigned_list__(details_node, class_name, 'dependencies')
        if len(deps) == 0:
            logger.info("Found no external dependencies that are required for class %s" % class_name)
        else:
            logger.info("Found the following external dependencies that are required for class {0}: {1}".format(class_name, deps))

        return deps


    def __get_processor_tags__(self, details_node, class_name):
        return self.__get_assigned_list__(details_node, class_name, 'tags')


    def __get_assigned_list__(self, details_node, class_name, element_name):
        assignment_nodes = self.__get_assignment_nodes__(details_node)
        for assignment_node in assignment_nodes:
            if (len(assignment_node.targets) == 1 and assignment_node.targets[0].id == element_name):
                assigned_values = assignment_node.value.elts
                declared_dependencies = []
                for assigned_value in assigned_values:
                    declared_dependencies.append(assigned_value.value)

                return declared_dependencies

        # No values found
        return []


    def __get_processor_description__(self, details_node, class_name):
        assignment_nodes = self.__get_assignment_nodes__(details_node)
        for assignment_node in assignment_nodes:
            if (len(assignment_node.targets) == 1 and assignment_node.targets[0].id == 'description'):
                return assignment_node.value.value

        # No description found
        logger.debug("Found no description for class %s" % class_name)
        return None



    def __get_class_nodes__(self, node):
        class_nodes = [n for n in node.body if isinstance(n, ast.ClassDef)]
        return class_nodes


    def __get_assignment_nodes__(self, node):
        assignment_nodes = [n for n in node.body if isinstance(n, ast.Assign)]
        return assignment_nodes


    def __import_external_dependencies__(self, class_name, dependencies, target_dir):
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        completion_marker_file = os.path.join(target_dir, 'dependency-download.complete')
        if os.path.exists(completion_marker_file):
            logger.info("All dependencies have already been imported for {0}".format(class_name))
            return True

        python_cmd = os.getenv("PYTHON_CMD")

        # todo remove this! Only for testing because need to hit 'test pypi server' When ready to use real one, just add to the 'args' below.
        install_api_cmd = [python_cmd, '-m', 'pip', 'install', '--no-deps', '-U', '-i', 'https://test.pypi.org/simple/', '--pre', '--target', target_dir, NIFI_API_DEPENDENCY]
        install_api_result = subprocess.run(install_api_cmd)
        if install_api_result.returncode == 0:
            logger.info("Successfully imported NiFi API for {0} to {1}".format(class_name, target_dir))
        else:
            logger.error("Failed to import NiFi API for {0}: process exited with status code {1}".format(class_name, install_api_result))

        # If no dependencies to import, just return
        if len(dependencies) == 0:
            # Write a completion Marker File
            file = open(completion_marker_file, "w")
            file.write("True")
            file.close()
            return True

        # todo end the remove this section




        # TODO: Need to find out the correct command to run for python, not just assume it's python3? Or is this correct, as it's run from within a virtual env?
        args = [python_cmd, '-m', 'pip', 'install', '--target', target_dir]
        for dep in dependencies:
            args.append(dep)

        logger.info("Importing dependencies {0} for {1} to {2} using command {3}".format(dependencies, class_name, target_dir, args))

        result = subprocess.run(args)
        if result.returncode == 0:
            logger.info("Successfully imported requirements for {0} to {1}".format(class_name, target_dir))

            # Write a completion Marker File
            file = open(completion_marker_file, "w")
            file.write("True")
            file.close()

            return True
        else:
            logger.error("Failed to import requirements for {0}: process exited with status code {1}".format(class_name, result))
            return False


    def __load_extension_module__(self, file, local_dependencies):
        # If there are any local dependencies (i.e., other python files in the same directory), load those modules first
        if local_dependencies is not None:
            for local_dependency in local_dependencies:
                if local_dependency == file:
                    continue

                logger.debug("Loading local dependency {0} before loading {1}".format(local_dependency, file))
                self.__load_extension_module__(local_dependency, None)


        # Determine the module name
        moduleName = Path(file).name.split('.py')[0]

        # Create the module specification
        moduleSpec = importlib.util.spec_from_file_location(moduleName, file)
        logging.debug('Module Spec: %s' % moduleSpec)

        # Create the module from the specification
        module = importlib.util.module_from_spec(moduleSpec)
        logger.debug('Module: %s' % module)

        # Initialize the JvmHolder class with the gateway jvm.
        # This must be done before executing the module to ensure that the nifiapi module
        # is able to access the JvmHolder.jvm variable. This enables the nifiapi.properties.StandardValidators, etc. to be used
        # However, we have to delay the import until this point, rather than adding it to the top of the ExtensionManager class
        # because we need to ensure that we've fetched the appropriate dependencies for the pyenv environment for the extension point.
        from nifiapi.__jvm__ import JvmHolder
        JvmHolder.jvm = self.gateway.jvm
        JvmHolder.gateway = self.gateway

        # Load the module
        sys.modules[moduleName] = module
        moduleSpec.loader.exec_module(module)
        logger.info("Loaded module %s" % moduleName)

        # Find the Processor class and return it
        for name, member in inspect.getmembers(module):
            if inspect.isclass(member):
                logger.debug('Found class: %s' % member)
                if self.__is_processor_class__(member):
                    logger.debug('Found Processor: %s' % member)
                    return member

        return None


    def __is_processor_class__(self, potentialProcessorClass):
        # Go through all members of the given class and see if it has an inner class named Java
        for name, member in inspect.getmembers(potentialProcessorClass):
            if name == 'Java' and inspect.isclass(member):
                # Instantiate the Java class
                instance = member()

                # Check if the instance has a method named 'implements'
                hasImplements = False
                for attr in dir(instance):
                    if attr == 'implements':
                        hasImplements = True
                        break

                # If not, move to the next member
                if not hasImplements:
                    continue

                # The class implements something. Check if it implements Processor
                for interface in instance.implements:
                    if interface in self.processorInterfaces:
                        logger.debug('%s implements Processor' % potentialProcessorClass)
                        return True
        return False
