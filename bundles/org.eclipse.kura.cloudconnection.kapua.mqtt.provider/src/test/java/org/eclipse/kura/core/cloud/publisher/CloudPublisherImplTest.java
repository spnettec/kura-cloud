/*******************************************************************************
 * Copyright (c) 2018, 2026 Eurotech and/or its affiliates and others
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
package org.eclipse.kura.core.cloud.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraErrorCode;
import org.junit.jupiter.api.Test;

class CloudPublisherImplTest {
    @Test
    void testPublishNoCloudService() {
        CloudPublisherImpl publisher = new CloudPublisherImpl();
        try {
            KuraException failure = assertThrows(KuraException.class, () -> publisher.publish(null));
            assertEquals(KuraErrorCode.SERVICE_UNAVAILABLE, failure.getCode());
        } finally {
            publisher.deactivate(null);
        }
    }
}
