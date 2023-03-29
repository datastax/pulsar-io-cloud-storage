# Release Datastax Pulsar IO Cloud Storage

```
mvn release:prepare \
    -DtagNameFormat="v@{project.version}" -DautoVersionSubmodules=true -Darguments=-DskipTests
```
