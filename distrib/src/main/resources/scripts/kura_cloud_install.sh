#!/bin/bash
#
# Copyright (c) 2026 YOFC IOT and others
#
# This program and the accompanying materials are made
# available under the terms of the Eclipse Public License 2.0
# which is available at https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#  YOFC IOT
#

STATUS=$1
BASE_DIR=$2
KURA_SYMLINK=$3

SIBLING_NAME="cloud"
REGISTRY="${BASE_DIR}/${KURA_SYMLINK}/framework/sibling-install-order"
# Write to kura_custom.properties (under user/) rather than kura.properties
# (under framework/). kura_custom.properties overrides kura.properties and
# is preserved across kura-core upgrades because user/ is part of the
# upgrade save/restore cycle.
KURA_PROPERTIES="${BASE_DIR}/${KURA_SYMLINK}/user/kura_custom.properties"

echo "Installing Kura Cloud addon..."
echo "  Status: ${STATUS}"
echo "  Base directory: ${BASE_DIR}"
echo "  Kura symlink: ${KURA_SYMLINK}"

# Register this sibling in the kura-core install-order registry
if [ -f "${REGISTRY}" ]; then
    if ! grep -qFx "${SIBLING_NAME}" "${REGISTRY}"; then
        echo "  Registering ${SIBLING_NAME} in sibling-install-order"
        echo "${SIBLING_NAME}" >> "${REGISTRY}"
    else
        echo "  ${SIBLING_NAME} already registered in sibling-install-order"
    fi
else
    echo "  Creating sibling-install-order registry"
    mkdir -p "${BASE_DIR}/${KURA_SYMLINK}/framework" 2>/dev/null || true
    echo "${SIBLING_NAME}" > "${REGISTRY}"
    chown kurad:kurad "${REGISTRY}" 2>/dev/null || true
    chmod 664 "${REGISTRY}" 2>/dev/null || true
fi

# Enable Cloud Connections UI tab: flip kura.have.cloud.connection to true.
# Write to kura_custom.properties so the setting survives kura-core upgrades.
echo "  Enabling cloud connection UI (kura.have.cloud.connection=true)"
mkdir -p "$(dirname "${KURA_PROPERTIES}")" 2>/dev/null || true
sed -i "s|^#*kura.have.cloud.connection=.*|kura.have.cloud.connection=true|" "${KURA_PROPERTIES}" 2>/dev/null \
    || echo "kura.have.cloud.connection=true" >> "${KURA_PROPERTIES}"

echo "Kura Cloud addon installation completed."
