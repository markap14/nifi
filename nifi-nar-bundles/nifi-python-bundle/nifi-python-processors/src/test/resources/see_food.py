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

from keras.applications.resnet50 import ResNet50
from keras.preprocessing import image
from keras.applications.resnet50 import preprocess_input, decode_predictions

import numpy as np
import io



class SeeFood(object):
    model = None

    def getRelationships(self):
        hotdog = gateway.jvm.org.apache.nifi.python.Relationship("hotdog", "hotdog")
        not_hotdog = gateway.jvm.org.apache.nifi.python.Relationship("not hotdog", "not hotdog")

        rels = [hotdog, not_hotdog]
        return SetConverter().convert(rels, gateway._gateway_client)

    def accept(self, flowFile):
        contents = flowFile.readContent()
        imgData = io.BytesIO(contents)

        img = image.load_img(imgData, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)

        if (self.model == None):
            self.model = ResNet50(weights='imagenet')

        preds = self.model.predict(x)

        decoded = decode_predictions(preds, top=1)[0]
        firstPrediction = decoded[0]
        name = firstPrediction[1]
        accuracy = firstPrediction[2]

        print("Name:")
        print(name)
        print("Accuracy:")
        print(accuracy)

        if (accuracy > 0.5 and name == "hotdog"):
            print "routing to hotdog"
            return "hotdog"
        else:
            print "routing to not hotdog"
            return "not hotdog"

    class Java:
        implements = ["org.apache.nifi.python.FlowFileOperator"]

operator = SeeFood()
gateway = ClientServer(
    java_parameters=JavaParameters(),
    python_parameters=PythonParameters(port=9999),
    python_server_entry_point=operator
)