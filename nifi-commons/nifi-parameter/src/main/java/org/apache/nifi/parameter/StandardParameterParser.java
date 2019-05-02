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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StandardParameterParser implements ParameterParser {
    private static final Logger logger = LoggerFactory.getLogger(StandardParameterParser.class);
    private static final char START_TAG = '#';
    private static final char OPEN_BRACE = '{';
    private static final char CLOSE_BRACE = '}';


    @Override
    public ParameterTokenList parseTokens(final String input) {
        if (input == null || input.isEmpty()) {
            return new StandardParameterTokenList(input, Collections.emptyList());
        }

        final List<ParameterToken> references = new ArrayList<>();

        int sequentialStartTags = 0;

        for (int i=0; i < input.length(); i++) {
            final char c = input.charAt(i);

            switch (c) {
                case START_TAG:
                    // If last character was a # character, then the previous character along with this character
                    // represent an escaped literal # character. Otherwise, this character potentially represents
                    // the start of a Parameter Reference.
                    sequentialStartTags++;
                    break;
                case OPEN_BRACE:
                    if (sequentialStartTags > 0) {
                        final ParameterToken token = parseParameterToken(input, i, sequentialStartTags, references);

                        // If we found a reference, skip 'i' to the end of the reference, since there can't be any other references before that point.
                        if (token != null) {
                            i = token.getEndOffset();
                        }
                    }

                    break;
                default:
                    break;
            }

            if (c != START_TAG) {
                sequentialStartTags = 0;
            }
        }

        logger.debug("For input {} found {} Parameter references: {}", input, references.size(), references);
        return new StandardParameterTokenList(input, references);
    }


    private ParameterToken parseParameterToken(final String input, final int startIndex, final int sequentialStartTags, final List<ParameterToken> tokens) {
        int startCharIndex = startIndex - sequentialStartTags;
        final int endCharIndex = input.indexOf(CLOSE_BRACE, startIndex);
        if (endCharIndex < 0) {
            return null;
        }


        final int numEscapedStartTags = (sequentialStartTags - 1)/2;
        final int startOffset = startCharIndex + numEscapedStartTags * 2;
        final String referenceText = input.substring(startOffset, endCharIndex + 1);

        // If we have multiple escapes before the start tag, we need to add a StartCharacterEscape for each one.
        // For example, if we have ###{foo}, then we should end up with a StartCharacterEscape followed by an actual Parameter Reference.
        for (int escapes=0; escapes < numEscapedStartTags; escapes++) {
            tokens.add(new StartCharacterEscape(startCharIndex + escapes * 2));
        }

        final ParameterToken token;
        if (sequentialStartTags % 2 == 1) {
            final String parameterName = input.substring(startCharIndex + sequentialStartTags + 1, endCharIndex);
            token = new StandardParameterReference(parameterName, startOffset, endCharIndex, referenceText);
        } else {
            token = new EscapedParameterReference(startOffset, endCharIndex, referenceText);
        }

        tokens.add(token);
        return token;
    }
}
