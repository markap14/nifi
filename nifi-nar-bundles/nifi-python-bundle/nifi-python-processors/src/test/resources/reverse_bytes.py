# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from py4j.java_collections import SetConverter
from py4j.clientserver import ClientServer, JavaParameters, PythonParameters


class Reverse(object):

    success = None

    def getRelationships(self):
        if self.success == None:
            self.success = gateway.jvm.org.apache.nifi.python.Relationship("success", "success")

        rels = [self.success]
        return SetConverter().convert(rels, gateway._gateway_client)

    def accept(self, flowFile):
        contents = flowFile.readContent()
        flowFile.writeContent(contents[::-1])
        return "success"

    class Java:
        implements = ["org.apache.nifi.python.FlowFileOperator"]

operator = Reverse()
gateway = ClientServer(
    java_parameters=JavaParameters(),
    python_parameters=PythonParameters(port=9999),
    python_server_entry_point=operator
)