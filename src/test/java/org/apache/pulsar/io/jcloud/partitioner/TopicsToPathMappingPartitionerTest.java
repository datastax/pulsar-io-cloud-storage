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
package org.apache.pulsar.io.jcloud.partitioner;

import com.google.common.base.Supplier;
import junit.framework.TestCase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * partitioner unit test.
 */
@RunWith(Parameterized.class)
public class TopicsToPathMappingPartitionerTest extends TestCase {

    @Parameterized.Parameter(0)
    public Partitioner<Object> partitioner;

    @Parameterized.Parameter(1)
    public String expected;

    @Parameterized.Parameter(2)
    public String expectedPartitionedPath;

    @Parameterized.Parameter(3)
    public Record<Object> pulsarRecord;

    @Parameterized.Parameters
    public static Object[][] data() {
        BlobStoreAbstractConfig blobStoreAbstractConfig1 = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig1.setTimePartitionDuration("1d");
        blobStoreAbstractConfig1.setTimePartitionPattern("yyyy-MM-dd");
        blobStoreAbstractConfig1.setSliceTopicPartitionPath(false);
        blobStoreAbstractConfig1.setWithTopicPartitionNumber(false);
        blobStoreAbstractConfig1.setTopicsToPathMapping("public/default/test=path1," +
                "public/default/test-partition-1=path2,public/default/test/1=path3");
        SimplePartitioner<Object> simplePartitioner1 = new SimplePartitioner<>();
        simplePartitioner1.configure(blobStoreAbstractConfig1);
        TimePartitioner<Object> dayPartitioner1 = new TimePartitioner<>();
        dayPartitioner1.configure(blobStoreAbstractConfig1);

        BlobStoreAbstractConfig blobStoreAbstractConfig2 = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig2.setTimePartitionDuration("1d");
        blobStoreAbstractConfig2.setTimePartitionPattern("yyyy-MM-dd");
        blobStoreAbstractConfig2.setSliceTopicPartitionPath(false);
        blobStoreAbstractConfig2.setWithTopicPartitionNumber(true);
        blobStoreAbstractConfig2.setTopicsToPathMapping("public/default/test=path1," +
                "public/default/test-partition-1=path2,public/default/test/1=path3");
        SimplePartitioner<Object> simplePartitioner2 = new SimplePartitioner<>();
        simplePartitioner2.configure(blobStoreAbstractConfig2);
        TimePartitioner<Object> dayPartitioner2 = new TimePartitioner<>();
        dayPartitioner2.configure(blobStoreAbstractConfig2);

        BlobStoreAbstractConfig blobStoreAbstractConfig3 = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig3.setTimePartitionDuration("1d");
        blobStoreAbstractConfig3.setTimePartitionPattern("yyyy-MM-dd");
        blobStoreAbstractConfig3.setSliceTopicPartitionPath(true);
        blobStoreAbstractConfig3.setWithTopicPartitionNumber(false);
        blobStoreAbstractConfig3.setTopicsToPathMapping("public/default/test=path1," +
                "public/default/test-partition-1=path2,public/default/test/1=path3");
        SimplePartitioner<Object> simplePartitioner3 = new SimplePartitioner<>();
        simplePartitioner3.configure(blobStoreAbstractConfig3);
        TimePartitioner<Object> dayPartitioner3 = new TimePartitioner<>();
        dayPartitioner3.configure(blobStoreAbstractConfig3);

        BlobStoreAbstractConfig blobStoreAbstractConfig4 = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig4.setTimePartitionDuration("1d");
        blobStoreAbstractConfig4.setTimePartitionPattern("yyyy-MM-dd");
        blobStoreAbstractConfig4.setSliceTopicPartitionPath(true);
        blobStoreAbstractConfig4.setWithTopicPartitionNumber(true);
        blobStoreAbstractConfig4.setTopicsToPathMapping("public/default/test=path1," +
                "public/default/test-partition-1=path2,public/default/test/1=path3");
        SimplePartitioner<Object> simplePartitioner4 = new SimplePartitioner<>();
        simplePartitioner4.configure(blobStoreAbstractConfig4);
        TimePartitioner<Object> dayPartitioner4 = new TimePartitioner<>();
        dayPartitioner4.configure(blobStoreAbstractConfig4);

        return new Object[][]{
                new Object[]{
                        simplePartitioner1,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner1,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        simplePartitioner1,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner1,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        simplePartitioner2,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner2,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        simplePartitioner2,
                        "3221225506",
                        "path2" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner2,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path2/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        simplePartitioner3,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner3,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        simplePartitioner3,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner3,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        simplePartitioner4,
                        "3221225506",
                        "path1" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner4,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path1/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getTopic()
                },
                new Object[]{
                        simplePartitioner4,
                        "3221225506",
                        "path3" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner4,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        "path3/2020-09-08" + Partitioner.PATH_SEPARATOR + "3221225506",
                        getPartitionedTopic()
                },
        };
    }

    public static Record<byte[]> getPartitionedTopic() {
        @SuppressWarnings("unchecked")
        Message<byte[]> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        String topic = TopicName.get("test-partition-1").toString();
        Record<byte[]> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        return mockRecord;
    }

    public static Record<Object> getTopic() {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        String topic = TopicName.get("test").toString();
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        return mockRecord;
    }

    @Test
    public void testEncodePartition() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expected, encodePartition);
    }

    @Test
    public void testGeneratePartitionedPath() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        String partitionedPath =
                partitioner.generatePartitionedPath(pulsarRecord.getTopicName().get(), encodePartition);

        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expectedPartitionedPath, partitionedPath);
    }
}
