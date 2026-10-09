/*******************************************************************************
 * Copyright (c) 2023, 2026 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Eurotech
 *******************************************************************************/
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.message.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.kura.cloudconnection.sparkplug.mqtt.message.SparkplugBProtobufPayloadBuilder;
import org.eclipse.tahu.protobuf.SparkplugBProto.DataType;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SparkplugBProtobufPayloadBuilderTest {

    private static final long TIMESTAMP = 123456789L;

    static Stream<TypeMapperCase> supportedTypes() {
        return Stream.of(
                testCase(false, DataType.Boolean),
                testCase("字节".getBytes(StandardCharsets.UTF_8), DataType.Bytes),
                testCase(11.2, DataType.Double),
                testCase(99.1F, DataType.Float),
                testCase((byte) -12, DataType.Int8),
                testCase((short) -1234, DataType.Int16),
                testCase(32, DataType.Int32),
                testCase(64L, DataType.Int64),
                testCase((short) 255, DataType.UInt8),
                testCase(65535, DataType.UInt16),
                testCase(4294967295L, DataType.UInt32),
                testCase(new BigInteger("18446744073709551615"), DataType.UInt64),
                testCase("a string", DataType.String),
                testCase("a text", DataType.Text),
                testCase("a uuid", DataType.UUID),
                testCase(new Date(TIMESTAMP), DataType.DateTime));
    }

    static Stream<TypeMapperCase> unsupportedTypes() {
        return Stream.of(DataType.DataSet, DataType.Template, DataType.PropertySet, DataType.PropertySetList,
                DataType.File, DataType.BooleanArray, DataType.DateTimeArray, DataType.UInt8Array,
                DataType.UInt64Array, DataType.UInt32Array, DataType.UInt16Array, DataType.StringArray,
                DataType.Int8Array, DataType.Int64Array, DataType.Int32Array, DataType.Int16Array,
                DataType.FloatArray, DataType.DoubleArray, DataType.Unknown)
                .map(type -> new TypeMapperCase("metric." + type, new Object(), TIMESTAMP, type,
                        new UnsupportedOperationException()));
    }

    static Stream<TypeMapperCase> inferredTypes() {
        return supportedTypes().filter(testCase -> !List.of(DataType.UInt8, DataType.UInt16, DataType.UInt32,
                DataType.Text, DataType.UUID).contains(testCase.getExpectedDataType()));
    }

    @ParameterizedTest
    @MethodSource("supportedTypes")
    void shouldBuildExplicitMetric(TypeMapperCase testCase) {
        Payload payload = new SparkplugBProtobufPayloadBuilder().withMetric(testCase.getName(), testCase.getValue(),
                testCase.getExpectedDataType(), testCase.getTimestamp()).buildPayload();

        assertMetric(testCase, payload);
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void shouldRejectUnsupportedMetric(TypeMapperCase testCase) {
        SparkplugBProtobufPayloadBuilder builder = new SparkplugBProtobufPayloadBuilder();
        assertThrows(testCase.getExpectedException().orElseThrow().getClass(),
                () -> builder.withMetric(testCase.getName(), testCase.getValue(), testCase.getExpectedDataType(),
                        testCase.getTimestamp()));
        assertEquals(0, builder.buildPayload().getMetricsCount());
    }

    @ParameterizedTest
    @MethodSource("inferredTypes")
    void shouldInferMetricType(TypeMapperCase testCase) {
        Payload payload = new SparkplugBProtobufPayloadBuilder().withMetric(testCase.getName(), testCase.getValue(),
                testCase.getTimestamp()).buildPayload();

        assertMetric(testCase, payload);
    }

    @Test
    void shouldReturnCorrectBdSeq() {
        Payload payload = new SparkplugBProtobufPayloadBuilder().withBdSeq(12L, 120L).buildPayload();
        assertMetric(new TypeMapperCase("bdSeq", 12L, 120L, DataType.Int64), payload);
    }

    @Test
    void shouldReturnCorrectSeq() {
        assertEquals(13L, new SparkplugBProtobufPayloadBuilder().withSeq(13L).buildPayload().getSeq());
    }

    @Test
    void shouldReturnCorrectTimestamp() {
        assertEquals(13123L, new SparkplugBProtobufPayloadBuilder().withTimestamp(13123L).buildPayload().getTimestamp());
    }

    @Test
    void shouldReturnCorrectBody() {
        byte[] body = "example.body".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(body, new SparkplugBProtobufPayloadBuilder().withBody(body).buildPayload().getBody().toByteArray());
    }

    @Test
    void shouldRoundTripSerializedPayload() throws Exception {
        SparkplugBProtobufPayloadBuilder builder = new SparkplugBProtobufPayloadBuilder()
                .withMetric("温度", -12.5, TIMESTAMP).withMetric("bytes", new byte[] { 0, -1, 42 }, TIMESTAMP)
                .withBdSeq(12, TIMESTAMP).withSeq(13).withTimestamp(TIMESTAMP).withBody(new byte[] { -1, 0 });

        assertEquals(builder.buildPayload(), Payload.parseFrom(builder.build()));
    }

    private static TypeMapperCase testCase(Object value, DataType type) {
        return new TypeMapperCase("metric." + type, value, TIMESTAMP, type);
    }

    private static void assertMetric(TypeMapperCase testCase, Payload payload) {
        assertEquals(1, payload.getMetricsCount());
        Metric metric = payload.getMetrics(0);
        assertEquals(testCase.getName(), metric.getName());
        assertEquals(testCase.getTimestamp(), metric.getTimestamp());
        assertEquals(testCase.getExpectedDataType().getNumber(), metric.getDatatype());
        Object value = testCase.getValue();
        switch (testCase.getExpectedDataType()) {
        case Boolean:
            assertEquals(value, metric.getBooleanValue());
            break;
        case Bytes:
            assertArrayEquals((byte[]) value, metric.getBytesValue().toByteArray());
            break;
        case Double:
            assertEquals(value, metric.getDoubleValue());
            break;
        case Float:
            assertEquals(value, metric.getFloatValue());
            break;
        case Int8:
        case Int16:
        case Int32:
        case UInt8:
        case UInt16:
            assertEquals(((Number) value).intValue(), metric.getIntValue());
            break;
        case Int64:
        case UInt32:
            assertEquals(value, metric.getLongValue());
            break;
        case UInt64:
            assertEquals(value.toString(), Long.toUnsignedString(metric.getLongValue()));
            break;
        case DateTime:
            assertEquals(((Date) value).getTime(), metric.getLongValue());
            break;
        case String:
        case Text:
        case UUID:
            assertEquals(value, metric.getStringValue());
            break;
        default:
            throw new AssertionError("Unexpected supported data type: " + testCase.getExpectedDataType());
        }
    }
}
