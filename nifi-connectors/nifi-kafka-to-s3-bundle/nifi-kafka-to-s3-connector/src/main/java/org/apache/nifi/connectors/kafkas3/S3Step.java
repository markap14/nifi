/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class S3Step {

    public static final ConnectorPropertyDescriptor S3_BUCKET = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Bucket")
        .description("The name of the S3 bucket to write data to.")
        .required(true)
        .build();

    public static final ConnectorPropertyDescriptor S3_REGION = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Region")
        .description("The AWS region where the S3 bucket is located.")
        // TODO - Use Connector Method to get list of regions
        .required(true)
        .build();

    public static final ConnectorPropertyDescriptor S3_DATA_FORMAT = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Data Format")
        .description("The format to use when writing data to S3.")
        .required(true)
        .defaultValue("JSON")
        .allowableValues("Avro", "JSON")
        .build();

    public static final ConnectorPropertyDescriptor S3_PREFIX = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Prefix")
        .description("An optional prefix to prepend to all object keys written to the S3 bucket.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor S3_ACCESS_KEY_ID = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Access Key ID")
        .description("The AWS Access Key ID used to authenticate to S3.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor S3_SECRET_ACCESS_KEY = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Secret Access Key")
        .description("The AWS Secret Access Key used to authenticate to S3.")
        .required(true)
        .type(PropertyType.PASSWORD)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();


    public static final ConnectorPropertyGroup S3_DESTINATION_GROUP = new ConnectorPropertyGroup.Builder()
        .name("S3 Destination Configuration")
        .description("Properties required to connect to S3 and specify the target bucket.")
        .properties(List.of(
            S3_BUCKET,
            S3_PREFIX,
            S3_REGION,
            S3_DATA_FORMAT
        ))
        .build();

    public static final ConnectorPropertyGroup S3_CREDENTIALS_GROUP = new ConnectorPropertyGroup.Builder()
        .name("S3 Credentials")
        .description("Properties required to authenticate to S3.")
        .properties(List.of(
            S3_ACCESS_KEY_ID,
            S3_SECRET_ACCESS_KEY
        ))
        .build();

    public static final ConfigurationStep S3_STEP = new ConfigurationStep.Builder()
        .name("S3 Configuration")
        .description("Configure connection to S3 and target bucket details.")
        .propertyGroups(List.of(
            S3_DESTINATION_GROUP,
            S3_CREDENTIALS_GROUP
        ))
        .build();
}
