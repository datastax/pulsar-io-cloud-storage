/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.s3.sink;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.s3.BlobStoreAbstractConfig;

/**
 * s3 sink configuration.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
public class S3SinkConfig extends BlobStoreAbstractConfig {

    private static final long serialVersionUID = 1245636479605735555L;

    private String accessKeyId;

    private String secretAccessKey;

    private String role;

    private String roleSessionName;

    public static S3SinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), S3SinkConfig.class);
    }

    public static S3SinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), S3SinkConfig.class);
    }

    @Override
    public String getProvider() {
        return "aws-s3";
    }

    @Override
    public void validate() {
        super.validate();
        checkNotNull(accessKeyId, "accessKeyId property not set.");
        checkNotNull(secretAccessKey, "secretAccessKey property not set.");
    }
}