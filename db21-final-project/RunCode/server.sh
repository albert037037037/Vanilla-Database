java \
-Dorg.elasql.config.file=elasql.properties \
-Dorg.elasql.bench.config.file=elasqlbench.properties \
-Dorg.vanilladb.comm.config.file=vanilladbcomm.properties \
-Dorg.vanilladb.bench.config.file=vanillabench.properties \
-Dorg.vanilladb.core.config.file=vanilladb.properties \
-Djava.util.logging.config.file=logging.properties \
-jar $1 \
$2 \
$3 \
$4 \
