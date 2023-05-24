### Setup Neo4j locally
1. Download standalone Neo4j Server from https://neo4j.com/download-center/ which requires Java 17.
2. Install Graph Data Science library from https://neo4j.com/docs/graph-data-science/current/installation/neo4j-server/. Copy the GDS jar to <neo4j-home>/plugins directory.
3. Install APOC library - copy from <neo4j-home>/labs folder to <neo4j-home>/plugins
4. In neo4j.conf, dbms.security.procedures.allowlist=apoc.create.*,gds.*
4. Start the neo4j server: <neo4j-home>/bin/neo4j console

### Run kmeans
1. Create a virtualenv and install requirements.txt
2. Copy data to load to <neo4j-home>/import directory.
2. python kmeans_gds.py
3. python kmeans_sagemaker.py

