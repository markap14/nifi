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

package org.apache.nifi.processors.openai;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;


@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Translates audio into English text. The audio data must be in one of these formats: flac, mp3, mp4, mpeg, mpga, m4a, ogg, wav, webm")
@Tags({"openai", "audio", "translate", "text", "speech-to-text", "mp3", "flac", "mp4", "mpeg", "mpga", "m4a", "ogg", "wav", "webm"})
public class OpenAiTranslate extends AbstractProcessor {
    private static final String URL = "https://api.openai.com/v1/audio/translations";
    private static final MediaType MULTIPART_FORM_DATA = MediaType.parse("multipart/form-data");

    static final AllowableValue RESPONSE_FORMAT_JSON = new AllowableValue("json", "JSON");
    static final AllowableValue RESPONSE_FORMAT_TEXT = new AllowableValue("text", "Text");
    static final AllowableValue RESPONSE_FORMAT_SRT = new AllowableValue("srt", "SRT");
    static final AllowableValue RESPONSE_FORMAT_VERBOSE_JSON = new AllowableValue("verbose_json", "Verbose JSON");
    static final AllowableValue RESPONSE_FORMAT_VTT = new AllowableValue("vtt", "VTT");

    private static final Map<String, String> MIME_TYPES = Map.of(
        RESPONSE_FORMAT_JSON.getValue(), "application/json",
        RESPONSE_FORMAT_TEXT.getValue(), "text/plain",
        RESPONSE_FORMAT_SRT.getValue(), "application/x-subrip",
        RESPONSE_FORMAT_VERBOSE_JSON.getValue(), "application/json",
        RESPONSE_FORMAT_VTT.getValue(), "text/vtt");

    private static final Map<String, String> FILENAME_EXTENSIONS = Map.of(
        RESPONSE_FORMAT_JSON.getValue(), ".json",
        RESPONSE_FORMAT_TEXT.getValue(), ".txt",
        RESPONSE_FORMAT_SRT.getValue(), ".srt",
        RESPONSE_FORMAT_VERBOSE_JSON.getValue(), ".json",
        RESPONSE_FORMAT_VTT.getValue(), ".vtt");


    static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
        .name("OpenAI API Key")
        .description("The API Key for interacting with OpenAI")
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor MODEL_NAME = new PropertyDescriptor.Builder()
        .name("Model Name")
        .description("The name of the OpenAI Model to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("whisper-1")
        .build();

    static final PropertyDescriptor PROMPT = new PropertyDescriptor.Builder()
        .name("Prompt")
        .description("Text that can be used to guide the model's style or continue a previous audio segment. The text must be in English.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor RESPONSE_FORMAT = new PropertyDescriptor.Builder()
        .name("Response Format")
        .description("Specifies which format is desired for the output")
        .required(true)
        .allowableValues(RESPONSE_FORMAT_JSON, RESPONSE_FORMAT_TEXT, RESPONSE_FORMAT_SRT, RESPONSE_FORMAT_VERBOSE_JSON, RESPONSE_FORMAT_VTT)
        .defaultValue(RESPONSE_FORMAT_JSON.getValue())
        .build();

    static final PropertyDescriptor TEMPERATURE = new PropertyDescriptor.Builder()
        .name("Temperature")
        .description("The sampling temperature to use. The value must be a floating-point number between 0.0 and 1.0. A higher value, such as 0.8 will result in more " +
                     "of an interpreted translation, whereas a value of 0.0 will result in a more literal translation.")
        .required(true)
        .addValidator(StandardValidators.createNonNegativeFloatingPointValidator(1.0D))
        .defaultValue("0")
        .build();


    private static final List<PropertyDescriptor> properties = List.of(
        API_KEY,
        MODEL_NAME,
        PROMPT,
        RESPONSE_FORMAT,
        TEMPERATURE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that have been successfully translated will be transferred to this relationship.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that could not be translated are routed to this relationship.")
        .build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);

    private OkHttpClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setupClient() {
        client = new OkHttpClient.Builder()
            .callTimeout(Duration.ofSeconds(60))
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(60))
            .writeTimeout(Duration.ofSeconds(60))
            .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String modelName = context.getProperty(MODEL_NAME).getValue();
        final String responseFormat = context.getProperty(RESPONSE_FORMAT).getValue();
        final String mimeType = MIME_TYPES.get(responseFormat);
        final String filenameExtension = FILENAME_EXTENSIONS.get(responseFormat);
        final String prompt = context.getProperty(PROMPT).evaluateAttributeExpressions(flowFile).getValue();
        final String temperature = context.getProperty(TEMPERATURE).getValue();

        final String responseText;
        try (final InputStream in = session.read(flowFile)) {
            final RequestBody audioRequestBody = new InputStreamRequestBody(flowFile.getSize(), in);
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final MultipartBody.Builder multipartBuilder = new MultipartBody.Builder()
                .setType(MULTIPART_FORM_DATA)
                .addFormDataPart("file", filename, audioRequestBody)
                .addFormDataPart("model", modelName)
                .addFormDataPart("response_format", responseFormat)
                .addFormDataPart("temperature", temperature);

            if (prompt != null) {
                multipartBuilder.addFormDataPart("prompt", prompt);
            }

            final MultipartBody multipartBody = multipartBuilder.build();

            final String apiToken = context.getProperty(API_KEY).getValue();
            final Request httpRequest = new Request.Builder()
                .addHeader("Authorization", "Bearer " + apiToken)
                .url(URL)
                .post(multipartBody)
                .build();

            final Call call = client.newCall(httpRequest);

            try (final Response response = call.execute()) {
                final String errorMessage = getErrorText(response);
                if (errorMessage != null) {
                    throw new IOException(errorMessage);
                }

                responseText = response.body().string();
            }
        } catch (final Exception e) {
            getLogger().error("Failed to translate {} into English; routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = Map.of(CoreAttributes.MIME_TYPE.key(), mimeType,
            CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + filenameExtension);

        flowFile = session.write(flowFile, out -> out.write(responseText.getBytes(StandardCharsets.UTF_8)));
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(flowFile, URL);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private String getErrorText(final Response response) throws IOException {
        final int responseCode = response.code();
        if (responseCode == 200) {
            return null;
        }

        final ResponseBody responseBody = response.body();
        final String body = (responseBody == null) ? null : responseBody.string();
        if (body == null || body.isBlank()) {
            return "Received non-200 Status code from OpenAI: " + responseCode;
        }

        return "Received non-200 Status code from OpenAI: " + responseCode + " with error: " + body;
    }

    private static class InputStreamRequestBody extends RequestBody {
        private final long contentLength;
        private final InputStream inputStream;

        public InputStreamRequestBody(final long contentLength, final InputStream inputStream) {
            this.contentLength = contentLength;
            this.inputStream = inputStream;
        }

        @Override
        public long contentLength() {
            return contentLength;
        }

        @Override
        public void writeTo(final BufferedSink bufferedSink) throws IOException {
            final Source source = Okio.source(inputStream);
            bufferedSink.writeAll(source);
        }

        @Override
        public MediaType contentType() {
            return null;
        }
    }
}

