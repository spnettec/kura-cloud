# Shared SQL and MQTT integration fixture

`BaseCloudTests` owns a private Moquette broker and creates real H2, DataService and
MqttDataTransport instances for each scenario. It verifies executor shutdown and
OSGi service usage release while mocking only registry, status, watchdog and
crypto boundaries needed to bootstrap that pipeline.

The cloud base provider attaches a `fixtures` test JAR containing only
`BaseCloudTests` and its nested transport adapter. Cross-repository endpoint tests
can consume that artifact with `type=test-jar`, `classifier=fixtures`, `scope=test`
after building this module with test compilation enabled. It is not part of the
production bundle or any runtime distribution.

Validation for fixture export: Maven 3.10.0 / JDK 21, all 57 cloud base tests and
installation pass; the fixture JAR contains exactly the two expected classes.

Endpoint suites may override `configureBrokerProperties(Properties)` for a bounded
large request limit. The default fixture retains Moquette defaults. This hook affects
test broker configuration only; production transport and broker configuration do not change.
