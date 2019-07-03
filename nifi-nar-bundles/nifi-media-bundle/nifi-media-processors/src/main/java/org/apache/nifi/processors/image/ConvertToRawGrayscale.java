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
package org.apache.nifi.processors.image;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ConvertToRawGrayscale extends AbstractProcessor {

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Images that are successfully converted will be routed to this relationship")
        .build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that cannot be converted will be routed to this relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final OutputStream out = new BufferedOutputStream(rawOut)) {

                        final ImageInputStream iis = ImageIO.createImageInputStream(in);
                        if (iis == null) {
                            throw new ProcessException("FlowFile is not in a valid format");
                        }

                        final Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
                        if (!readers.hasNext()) {
                            throw new ProcessException("FlowFile is not in a valid format");
                        }

                        final ImageReader reader = readers.next();
                        reader.setInput(iis, true);
                        final BufferedImage image = reader.read(0);

                        final BufferedImage grayImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
                        final Graphics graphics = grayImage.getGraphics();
                        graphics.drawImage(image, 0, 0, null);
                        graphics.dispose();

                        final byte[] line = new byte[grayImage.getWidth()];
                        for (int y=0; y < grayImage.getHeight(); y++) {
                            for (int x=0; x < grayImage.getWidth(); x++) {
                                final int rgb = grayImage.getRGB(x, y);
                                final int blue = rgb & 0xFF;
                                final int green = (rgb >> 8) & 0xFF;
                                final int red = (rgb >> 16) & 0xFF;

                                final int value = (blue + green + red) / 3;
                                line[x] = (byte) value;
                            }

                            out.write(line);
                        }
                    }
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to resize {} due to {}", new Object[] { flowFile, e });
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
