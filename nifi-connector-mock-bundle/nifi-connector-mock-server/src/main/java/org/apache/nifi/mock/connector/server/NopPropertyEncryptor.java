/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.encrypt.PropertyEncryptor;

public class NopPropertyEncryptor implements PropertyEncryptor {
    @Override
    public String encrypt(final String property) {
        return property;
    }

    @Override
    public String decrypt(final String encryptedProperty) {
        return encryptedProperty;
    }
}
