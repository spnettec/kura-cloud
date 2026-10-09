/*******************************************************************************
 * Copyright (c) 2017, 2026 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Eurotech
 ******************************************************************************/
package org.eclipse.kura.core.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


import org.eclipse.kura.message.KuraBirthPayload;
import org.eclipse.kura.message.KuraBirthPayload.KuraBirthPayloadBuilder;
import org.eclipse.kura.message.KuraBirthPayload.TamperStatus;
import org.eclipse.kura.message.KuraDeviceProfile;
import org.eclipse.kura.message.KuraPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class KuraBirthPayloadTest {

    @Nested
    class BirthPayloadTest extends StepsCollection {

        @Test
        public void shouldReturnBirthPayloadWithDefaults() {
            givenBuilderWithDefaultRequiredValues();

            whenKuraPayloadBuilderBuild();

            thenBirthPayloadContainsAllDefaultRequiredValues();
            thenBirthPayloadContainsApplicationFramework(KuraDeviceProfile.DEFAULT_APPLICATION_FRAMEWORK);
            thenBirthPayloadContainsTamperStatus(TamperStatus.UNSUPPORTED);
            thenBirthPayloadNotContainsKuraPosition();
            thenBirthPayloadNotContainsJdkVendorVersion();
        }

        @Test
        public void shouldReturnBirthPayloadWithCorrectPosition() {
            givenBuilderWithDefaultRequiredValues();
            givenBuilderWithPosition(200.0, 300.0);

            whenKuraPayloadBuilderBuild();

            thenBirthPayloadContainsPosition(200.0, 300.0);
        }

        @Test
        public void shouldReturnBirthPayloadWithCorrectApplicationFramework() {
            givenBuilderWithDefaultRequiredValues();
            givenBuilderWithApplicationFramework("test123");

            whenKuraPayloadBuilderBuild();

            thenBirthPayloadContainsApplicationFramework("test123");
        }

        @Test
        public void shouldReturnBirthPayloadWithJdkVendorVersion() {
            givenBuilderWithDefaultRequiredValues();
            givenBuilderWithJdkVendorVersion("JDK 123");

            whenKuraPayloadBuilderBuild();

            thenBirthPayloadContainsJdkVendorVersion("JDK 123");
        }

    }

    @Nested
    class TamperStatusParametricTest extends StepsCollection {

        @ParameterizedTest
        @EnumSource(TamperStatus.class)
        void shouldReturnBirthPayloadWithCorrectTamperStatus(TamperStatus testedTamperStatus) {
            givenBuilderWithDefaultRequiredValues();
            givenBuilderWithTamperStatus(testedTamperStatus);

            whenKuraPayloadBuilderBuild();

            thenBirthPayloadContainsTamperStatus(testedTamperStatus);
        }

        @ParameterizedTest
        @EnumSource(TamperStatus.class)
        void shouldReturnCorrectToStringRepresentation(TamperStatus testedTamperStatus) {
            givenBuilderWithDefaultRequiredValues();
            givenBuilderWithTamperStatus(testedTamperStatus);
            givenBuilderWithJdkVendorVersion("JDK 123");
            givenBuilderWithApplicationFramework("test123");
            givenBuilderWithPosition(200.0, 300.0);

            whenToString();

            thenToStringRepresentationIsCorrect();
        }

    }

    static class StepsCollection {

        private static final String DEFAULT_TEST_VALUE = "value";

        private KuraBirthPayloadBuilder builder;
        private KuraBirthPayload birthPayload;
        private String toStringRepresentation;

        /*
         * Given
         */

        void givenBuilderWithDefaultRequiredValues() {
            this.builder = new KuraBirthPayloadBuilder();
            this.builder.withUptime(DEFAULT_TEST_VALUE);
            this.builder.withDisplayName(DEFAULT_TEST_VALUE);
            this.builder.withAvailableProcessors(DEFAULT_TEST_VALUE);
            this.builder.withTotalMemory(DEFAULT_TEST_VALUE);
            this.builder.withOsArch(DEFAULT_TEST_VALUE);
            this.builder.withOsgiFramework(DEFAULT_TEST_VALUE);
            this.builder.withOsgiFrameworkVersion(DEFAULT_TEST_VALUE);
            this.builder.withModelName(DEFAULT_TEST_VALUE);
            this.builder.withModelId(DEFAULT_TEST_VALUE);
            this.builder.withPartNumber(DEFAULT_TEST_VALUE);
            this.builder.withSerialNumber(DEFAULT_TEST_VALUE);
            this.builder.withFirmwareVersion(DEFAULT_TEST_VALUE);
            this.builder.withBiosVersion(DEFAULT_TEST_VALUE);
            this.builder.withCpuVersion(DEFAULT_TEST_VALUE);
            this.builder.withOs(DEFAULT_TEST_VALUE);
            this.builder.withOsVersion(DEFAULT_TEST_VALUE);
            this.builder.withJvmName(DEFAULT_TEST_VALUE);
            this.builder.withJvmVersion(DEFAULT_TEST_VALUE);
            this.builder.withJvmProfile(DEFAULT_TEST_VALUE);
            this.builder.withKuraVersion(DEFAULT_TEST_VALUE);
            // this.builder.withApplicationFramework(DEFAULT_TEST_VALUE);
            this.builder.withApplicationFrameworkVersion(DEFAULT_TEST_VALUE);
            this.builder.withConnectionInterface(DEFAULT_TEST_VALUE);
            this.builder.withConnectionIp(DEFAULT_TEST_VALUE);
            this.builder.withAcceptEncoding(DEFAULT_TEST_VALUE);
            this.builder.withApplicationIdentifiers(DEFAULT_TEST_VALUE);
            this.builder.withModemImei(DEFAULT_TEST_VALUE);
            this.builder.withModemIccid(DEFAULT_TEST_VALUE);
            this.builder.withModemImsi(DEFAULT_TEST_VALUE);
            this.builder.withModemRssi(DEFAULT_TEST_VALUE);
            this.builder.withModemFirmwareVersion(DEFAULT_TEST_VALUE);
            // this.builder.withPosition(position);
            this.builder.withPayloadEncoding(DEFAULT_TEST_VALUE);
            // this.builder.withTamperStatus(status);
            this.builder.withJvmVendor(DEFAULT_TEST_VALUE);
            this.builder.withJdkVendorVersion(null);
        }

        void givenBuilderWithApplicationFramework(String applicationFramework) {
            this.builder.withApplicationFramework(applicationFramework);
        }

        void givenBuilderWithTamperStatus(TamperStatus status) {
            this.builder.withTamperStatus(status);
        }

        void givenBuilderWithPosition(double latitude, double longitude) {
            KuraPosition position = new KuraPosition();
            position.setLatitude(latitude);
            position.setLongitude(longitude);
            this.builder.withPosition(position);
        }

        void givenBuilderWithJdkVendorVersion(String jdkVendorVersion) {
            this.builder.withJdkVendorVersion(jdkVendorVersion);
        }

        /*
         * When
         */

        void whenKuraPayloadBuilderBuild() {
            this.birthPayload = this.builder.build();
        }

        void whenToString() {
            whenKuraPayloadBuilderBuild();
            this.toStringRepresentation = this.birthPayload.toString();
        }

        /*
         * Then
         */

        void thenBirthPayloadContainsAllDefaultRequiredValues() {
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getUptime());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getDisplayName());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getAvailableProcessors());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getTotalMemory());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getOsArch());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getOsgiFramework());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getOsgiFrameworkVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModelName());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModelId());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getPartNumber());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getSerialNumber());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getFirmwareVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getBiosVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getCpuVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getOs());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getOsVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getJvmName());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getJvmVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getJvmProfile());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getKuraVersion());
            // this.birthPayload.getApplicationFramework();
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getApplicationFrameworkVersion());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getConnectionInterface());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getConnectionIp());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getAcceptEncoding());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getApplicationIdentifiers());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModemImei());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModemIccid());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModemImsi());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModemRssi());
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getModemFirmwareVersion());
            // this.birthPayload.getPosition();
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getPayloadEncoding());
            // this.birthPayload.getTamperStatus();
            assertEquals(DEFAULT_TEST_VALUE, this.birthPayload.getJvmVendor());
            // this.birthPayload.getJdkVendorVersion();
        }

        void thenBirthPayloadContainsApplicationFramework(String expectedApplicationFramework) {
            assertEquals(expectedApplicationFramework, this.birthPayload.getApplicationFramework());
        }

        void thenBirthPayloadContainsTamperStatus(TamperStatus expectedTamperStatus) {
            assertEquals(expectedTamperStatus, this.birthPayload.getTamperStatus());
        }

        void thenBirthPayloadNotContainsKuraPosition() {
            assertNull(this.birthPayload.getPosition());
        }

        void thenBirthPayloadNotContainsJdkVendorVersion() {
            assertNull(this.birthPayload.getJdkVendorVersion());
        }

        void thenBirthPayloadContainsPosition(Double expectedLatitude, Double expectedLongitude) {
            KuraPosition position = this.birthPayload.getPosition();
            assertNotNull(position);
            assertEquals(expectedLatitude, position.getLatitude());
            assertEquals(expectedLongitude, position.getLongitude());
        }

        void thenBirthPayloadContainsJdkVendorVersion(String expectedJdkVendorVersion) {
            assertEquals(expectedJdkVendorVersion, this.birthPayload.getJdkVendorVersion());
        }

        void thenToStringRepresentationIsCorrect() {
            assertTrue(this.toStringRepresentation.startsWith("KuraBirthPayload ["));
            assertTrue(this.toStringRepresentation.endsWith("]"));

            assertEquals(30, this.toStringRepresentation.split(",").length, "Some properties are missing in the toString method.");

            for (String keyValueField : this.toStringRepresentation.split(",")) {
                assertTrue(keyValueField.contains("="));
            }
        }

    }

}
