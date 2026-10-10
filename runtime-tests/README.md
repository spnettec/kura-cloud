# Cloud factory runtime tests

JUnit 5 starts an isolated Equinox framework with actual SCR, ConfigurationAdmin,
ConfigurationService, CryptoService, cloud factory, CloudService, DataService,
MQTT transport and publisher bundles. Host system/status/watchdog services are
controlled boundaries. This suite does not connect a broker or validate SQL/TLS.

The two parameter invocations verify stack existence with and without a publisher.
Local custom `kura.service.pid`, `service.factoryPid` discovery, Chinese
`kura.cloud.factory.name` / `kura.cloud.factory.desc`, linked service targets,
publisher tracking and service removal are asserted separately. Do not replace
these local contracts with upstream PID-prefix or display-name assumptions.

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
