**To build presto-hive module only perform the following steps:**

```bash
cd <presto-project-root>
# Resolve dependencies - do it once
./mvnw dependency:resolve
# Clean and Build the presto-hive module only (without running tests)
./mvnw -pl presto-hive clean package -DskipTests

```
