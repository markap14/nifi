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
package org.apache.nifi.nar;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StandardExtensionDefinition implements ExtensionDefinition {
    private final ExtensionType extensionType;
    private final Class<?> extensionClass;
    private final Bundle bundle;
    private final String extensionName;
    private final String capabilityDescription;
    private final boolean restricted;
    private final String usageRestriction;
    private final Set<Restriction> explicitRestrictions;
    private final String deprecationReason;
    private final Collection<String> tags;

    private StandardExtensionDefinition(final ExtensionType extensionType, final Class<?> extensionClass, final Bundle bundle, final String extensionName, final String capabilityDescription,
                                       final boolean restricted, final String usageRestriction, final Set<Restriction> explicitRestrictions, final String deprecationReason,
                                       final Collection<String> tags) {
        this.extensionType = extensionType;
        this.extensionClass = extensionClass;
        this.bundle = bundle;
        this.extensionName = extensionName;
        this.capabilityDescription = capabilityDescription;
        this.restricted = restricted;
        this.usageRestriction = usageRestriction;
        this.explicitRestrictions = explicitRestrictions;
        this.deprecationReason = deprecationReason;
        this.tags = tags;
    }

    @Override
    public ExtensionType getExtensionType() {
        return extensionType;
    }

    @Override
    public Class<?> getExtensionClass() {
        return extensionClass;
    }

    @Override
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public String getExtensionName() {
        return extensionName;
    }

    @Override
    public String getCapabilityDescription() {
        return capabilityDescription;
    }

    @Override
    public boolean isRestricted() {
        return restricted;
    }

    @Override
    public String getUsageRestriction() {
        return usageRestriction;
    }

    @Override
    public Set<Restriction> getExplicitRestrictions() {
        return explicitRestrictions;
    }

    @Override
    public String getDeprecationReason() {
        return deprecationReason;
    }

    @Override
    public Collection<String> getTags() {
        return tags;
    }

    public static StandardExtensionDefinition forProcessor(final Class<?> processorClass, final Bundle bundle) {
        final String className = processorClass.getName();
        final String description = getCapabilityDescription(processorClass);
        final boolean restricted = isRestricted(processorClass);
        final String usageRestriction = getUsageRestriction(processorClass);
        final Set<Restriction> restrictions = getExplicitRestrictions(processorClass);
        final String deprecationReason = getDeprecationReason(processorClass);
        final Collection<String> tags = getTags(processorClass);

        return new StandardExtensionDefinition(ExtensionType.PROCESSOR, processorClass, bundle, className, description, restricted, usageRestriction, restrictions, deprecationReason, tags);
    }

    private static Set<Restriction> getExplicitRestrictions(final Class<?> cls) {
        final Restricted restricted = cls.getAnnotation(Restricted.class);

        if (restricted == null) {
            return null;
        }

        final Restriction[] restrictions = restricted.restrictions();

        if (restrictions == null || restrictions.length == 0) {
            return Collections.emptySet();
        }

        return new HashSet<>(Arrays.asList(restrictions));
    }

    private static String getDeprecationReason(final Class<?> cls) {
        final DeprecationNotice deprecationNotice = cls.getAnnotation(DeprecationNotice.class);
        return deprecationNotice == null ? null : deprecationNotice.reason();
    }

    private static Set<String> getTags(final Class<?> cls) {
        final Set<String> tags = new HashSet<>();
        final Tags tagsAnnotation = cls.getAnnotation(Tags.class);
        if (tagsAnnotation != null) {
            for (final String tag : tagsAnnotation.value()) {
                tags.add(tag);
            }
        }

        if (cls.isAnnotationPresent(Restricted.class)) {
            tags.add("restricted");
        }

        return tags;
    }

    private static String getCapabilityDescription(final Class<?> cls) {
        final CapabilityDescription capabilityDesc = cls.getAnnotation(CapabilityDescription.class);
        return capabilityDesc == null ? null : capabilityDesc.value();
    }

    private static boolean isRestricted(final Class<?> cls) {
        return cls.isAnnotationPresent(Restricted.class);
    }

    private static String getUsageRestriction(final Class<?> cls) {
        final Restricted restricted = cls.getAnnotation(Restricted.class);

        if (restricted == null) {
            return null;
        }

        if (StringUtils.isBlank(restricted.value())) {
            return null;
        }

        return restricted.value();
    }
}
