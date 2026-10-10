# Cloud factory runtime tests

JUnit 5 starts an isolated Equinox framework with actual SCR, ConfigurationAdmin,
ConfigurationService, CryptoService, cloud factory, CloudService, DataService,
MQTT transport, H2 database and publisher bundles. Host system/status/watchdog
services are controlled boundaries. The suite verifies actual in-memory SQL and
authenticated MQTT over a random loopback port; file persistence/TLS remain outside
this suite.

Five parameter invocations cover stack existence, publisher registration, JSON MQTT,
Protobuf MQTT and tamper-triggered birth publication. The transport cases queue a publication while disconnected,
connect through real configuration/crypto/data services, then check broker receipt
and the publisher's confirmation ID. CloudClient data/control round trips check all
six publication/confirmation/arrival callbacks, Unicode bodies, timestamps, metrics,
QoS and retain. ConfigurationAdmin stores an encrypted password that the real
CryptoService can decrypt.

Local custom `kura.service.pid`, `service.factoryPid` discovery, Chinese
`kura.cloud.factory.name` / `kura.cloud.factory.desc`, linked service targets,
publisher tracking and service removal are asserted separately. Do not replace
these local contracts with upstream PID-prefix or display-name assumptions.

H2 database and server factories must be separately discoverable. Assertions check
typed database defaults, English/Chinese metadata and six database attributes.
This catches the historical H2DbService XML that incorrectly duplicated H2DbServer.

Moquette 0.18 rejects client publications beginning with `$`; these test instances
use the existing configurable `EDC` control prefix. Production defaults are unchanged.
The broker requires fixed test credentials and disables persistence and telemetry.
Kura APIs remain absent from the controller classpath; its Moquette/H2/Paho libraries
serve only the external broker/observer. Business services run in actual bundles.

The tamper variant registers a simulated sensor through OSGi and observes actual
SCR binding. Initial MQTT BIRTH reports `NOT_TAMPERED`; an actual EventAdmin event
then triggers `TAMPERED` BIRTH after the production 30-second delay. Neither the
executor nor CloudService private state is replaced. The sensor is unregistered
at teardown. This validates the event pipeline, not physical tamper hardware.

After installing core and cloud artifacts into the same Maven cache:

```sh
mvn -f runtime-tests/pom.xml clean verify
# Or include this module in the cloud/IDEA reactor:
mvn -Posgi-it verify
```

Use Maven 3.10 and JDK 21. `kura/build-all.sh` invokes this suite after sibling
installation when `RUN_IT=1`; `-DskipITs` skips Failsafe. Business JARs are copied
as bundles, not put on the test controller classpath. Reports are in
`target/failsafe-reports`. Temporary configuration, snapshots and framework storage
are isolated from the user's Kura runtime.
