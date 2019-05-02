/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.parameter;

// TODO: Consider refactoring API here.
// We are including actual Parameter References as well as escaped non-references.
// We need to probably change that
public interface ParameterToken {
    int getStartOffset();

    int getEndOffset();

    String getText();

    boolean isEscapeSequence();

    boolean isParameterReference();

    /**
     * Returns the 'value' of the token. If this token is a parameter reference, it will return the value of the
     * Parameter, according to the given Parameter Context. If this token is an Escape Sequence, it will return the
     * un-escaped version of the escape sequence.
     * @param lookup the Parameter Lookup to use for looking up values
     * @return the value of the Token
     */
    String getValue(ParameterLookup lookup);
}
