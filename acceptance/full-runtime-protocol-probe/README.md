# Complete Mac runtime protocol acceptance

Test-only module outside the default cloud reactor. A separate authenticated
Moquette broker and an independent Paho client exercise the complete Maven
macOS application. The probe uses production host SystemService, configuration,
OCD/SCR, Kapua cloud factory, JSON/XML codecs and Felix DeploymentAdmin.

It first invokes six unchanged Jupiter-backed legacy assertions inside the
upstream fixture bundle, then sends twelve UUID-correlated MQTT requests:
CONF-V1 GET/PUT/snapshot/rollback, INVENTORY-V1 package/bundle/merged/system
queries, and CONF-V2 encrypted snapshot write/read. It asserts actual SCR update
and rollback, live deployment bundle inventory, Chinese cloud name/description
and owned cloud/deployment cleanup. It does not create a fake SystemService or
replace host package data with a DEB fixture.

## Run

Use Maven 3.10 and JDK 21 with the same absolute cache as the assembled runtime:

```sh
mvn -f acceptance/full-runtime-protocol-probe/pom.xml package dependency:build-classpath \
  -DincludeScope=test -Dmdep.outputFile=target/broker-classpath.txt \
  -DskipTests -DskipITs -Dmaven.repo.local=/absolute/cache

python3 acceptance/full-runtime-protocol-probe/run.py \
  --runtime /absolute/stopped-dedicated-runtime \
  --template-profile /absolute/isolated-template-profile \
  --fixtures /absolute/kura-osgi-tests/target/config-it-bundles \
  --core-repository /absolute/kura \
  --archive /absolute/new-acceptance-archive \
  --java /absolute/jdk21/bin/java
```

The template must be an isolated macOS acceptance profile on ports
18480/18443/18444. The runner creates a new owned data home/configuration area,
relocates bootstrap keystore references and copies current test fixture JARs.
The broker binds a random loopback port. It uses public isolated test credentials
and a one-MiB packet limit for the complete application inventory. Both owned
JVMs are awaited after cleanup; the broker stops by stdin EOF. A timeout or
forced shutdown fails acceptance. Failed runs are retained and never retried
automatically. No existing runtime/profile is cleaned or reassembled.

The six reflectively invoked fixture methods are separate from JUnit engine
reports. The twelve requests are protocol assertions, not twelve new JUnit
invocations. Do not add either count to overlapping historical workspace totals.
The direct legacy registry fixtures are closed before the MQTT phase.

The Mac system-package response is compared with the actual host API count.
An empty successful result does not prove Linux package-manager enumeration.
The development snapshot profile may use the default test encryption key;
production key provisioning has separate scope. Installed Debian, hardware,
D-Bus, GPU and additional Linux acceptance are not claimed. Production OSGi
metadata and YOFC/PLC4X/OPC UA behavior are unchanged.

2026-10-10 final evidence is recorded in the core repository's
`docs/testing/mac-complete-protocol-validation-20261010.json` and matching Markdown.
The preserved initial run exposed an immutable password Map in this test helper;
the corrected helper follows the existing fixture's mutable Map update pattern.
