Follow below steps to install a single node influxbd, graphite db and grafana for collecting spark metrics.

# Influxdb Setup
## Step 1 

```
curl -O https://dl.influxdata.com/influxdb/releases/influxdb-1.8.10.x86_64.rpm

Download Repo-

https://docs.influxdata.com/influxdb/v2.7/install/?t=Linux

yum install influxdb-1.8.10.x86_64.rpm
```

## Step 2

mv /etc/influxdb/influxdb.conf /etc/influxdb/influxdb.conf_orig

vim /etc/influxdb/influxdb.conf

Copy the below content in influxdb.conf

```

# influxdb conf

[meta]
  dir = "/influxdbdata/influxdb/meta"

[data]
  dir = "/influxdbdata/influxdb/data"
  engine = "tsm1"
  wal-dir = "/influxdbdata/influxdb/wal"

# Note Grafana http endpoint is on port 8086 by default.

[[graphite]]
  enabled = true
  bind-address = ":2003"
  database = "graphite"
  retention-policy = ""
  protocol = "tcp"
  batch-size = 5000
  batch-pending = 10
  batch-timeout = "1s"
  consistency-level = "one"
  separator = "."
  udp-read-buffer = 0
  templates = [
    # JVM source
    "*.*.jvm.pools.* username.applicationid.process.namespace.namespace.measurement*",
    # YARN source
    "*.*.applicationMaster.* username.applicationid.namespace.measurement*",
    # shuffle service source
    "*.shuffleService.* username.namespace.measurement*",
    # streaming
    "*.*.*.spark.streaming.* username.applicationid.process.namespace.namespace.id.measurement*",
    # generic template for driver and executor sources
    "username.applicationid.process.namespace.measurement*" ]

```

## Step 3 - Grafana Setup

```
curl -O https://dl.grafana.com/oss/release/grafana-9.5.2-1.x86_64.rpm

yum install grafana-9.5.2-1.x86_64.rpm

```
cd /etc/grafana/provisioning/datasources/


vim influx.yaml

```
apiVersion: 1

datasources:
- name: influx-sparkmeasure
  type: influxdb
  access: proxy
  orgId: 1
  url: http://localhost:8086
  password:
  user:
  database:
    sparkmeasure
  basicAuth:
  basicAuthUser:
  basicAuthPassword:
  withCredentials:
  isDefault:
  version: 1
  editable: true
- name: influx-graphite
  type: influxdb
  access: proxy
  orgId: 1
  url: http://localhost:8086
  password:
  user:
  database:
    graphite
  basicAuth:
  basicAuthUser:
  basicAuthPassword:
  withCredentials:
  isDefault:
  version: 1
  editable: true

```
vim spark.yaml

```
apiVersion: 1

providers:
- name: spark-dashboard
  orgId: 1
  folder: ''
  folderUid: ''
  type: file
  disableDeletion: false
  editable: true
  updateIntervalSeconds: 10
  options:
    path: /var/lib/grafana/dashboards
```


chown -R grafana:grafana /etc/grafana/provisioning

chown -R grafana:grafana /var/lib/grafana

## Step 4 - Start the Services - 

```
INFLUXDB -

systemctl start influxdb.service
systemctl status influxdb.service
systemctl enable influxdb.service

Check Logs

sudo journalctl -u influxdb.service

GRAFANA - 

systemctl start grafana-server.service
systemctl status grafana-server.service
systemctl enable grafana-server.service

```

Verify the Grafana running on the server http://<<Grafana-host-IP>>:3000


## Step 5 

Import the grafana dashboard

sparkmetrics/spark_metrics_grafana_dashboard.json

## Step 6 

### How to configure spark job to send metrics to graphite.


#### Step 1. Create the config file - metrics.properties

```
spark.metrics.conf.*.sink.graphite.class org.apache.spark.metrics.sink.GraphiteSink
spark.metrics.conf.*.sink.graphite.host <graphite-host-ip>
spark.metrics.conf.*.sink.graphite.port 2003
spark.metrics.conf.*.sink.graphite.period 10
spark.metrics.conf.*.sink.graphite.unit seconds
spark.metrics.conf.*.sink.graphite.prefix <env-name> 
spark.metrics.conf.*.source.jvm.class org.apache.spark.metrics.source.JvmSource
spark.metrics.staticSources.enabled true
spark.metrics.appStatusSource.enabled true
```

#### Step 2. Run your spark submit job using the above config. NOTE - Below is the sample job tested.

```
spark-submit **--properties-file metrics.properties** --class NameCountJob --master yarn --deploy-mode cluster --driver-memory 2g --executor-memory 2g --executor-cores 1 --conf 'spark.dynamicAllocation.enabled=true' --conf 'spark.shuffle.service.enabled=true' --conf 'spark.yarn.max.executor.faiures=9' --conf 'spark.dynamicAllocation.maxExecutors=10' --conf 'spark.dynamicAllocation.minExecutors=1' --conf 'spark.dynamicAllocation.initialExecutors=1' --conf 'spark.dynamiAllocation.executorIdleTimeout=60' --conf 'spark.shuffle.service.port=7337' --conf spark.app.name=NameCountJob /home/scripts/spark-count-output_2/NameCountJob.jar hdfs://dev/tmpdir/metric_mon/input/sample_data5.csv hdfs://dev/tmpdir/metric_mon/output/$(date +%Y%m%d%H%M%S)
```

Once spark jobs is started, tables and data will be populated to graphite db which can be verified as below.
The database graphite in influxdb will be automatically be created on the metrics is populated.

```
    [root@rosgrafanainfluxnode01 ~]# influx
    Connected to http://localhost:8086 version 1.8.10
    InfluxDB shell version: 1.8.10
    > SHOW DATABASES;
    name: databases
    name
    ----
    _internal
    graphite
    > use grphite;
    > select * from "autogen"."threadpool.completeTasks" limit 10;
name: threadpool.completeTasks
time                applicationid                  namespace process username  value
----                -------------                  --------- ------- --------  -----
1688561883000000000 application_1688469712789_0321 executor  1       dev 18
1688561884000000000 application_1688469712789_0321 executor  2       dev 13

>
```

#### Set the retention policy of graphite db based on how many days/weeks data needs to be stored.

```
> use graphite
Using database graphite
> CREATE RETENTION POLICY db_retention_policy ON graphite DURATION 2d REPLICATION 1
> SHOW RETENTION POLICIES
name                duration shardGroupDuration replicaN default
----                -------- ------------------ -------- -------
autogen             0s       168h0m0s           1        true
db_retention_policy 48h0m0s  24h0m0s            1        false
>
```

## To view data with Human readable timestamp.

```

> precision rfc3339
> select * from "autogen"."threadpool.completeTasks" limit 10;
name: threadpool.completeTasks
time                 applicationid                  namespace process username  value
----                 -------------                  --------- ------- --------  -----
2023-07-05T12:58:03Z application_1688469712789_0321 executor  1       dev 18
2023-07-05T12:58:04Z application_1688469712789_0321 executor  2       dev 13
2023-07-05T12:58:06Z application_1688469712789_0321 executor  10      dev 13
2023-07-05T12:58:06Z application_1688469712789_0321 executor  3       dev 14
>
```

## Drop the database.

1. Stop Grafana
2. Drop the database and restart influxdb service.

```
[root@rosdevinfluxdbgrafana01 ~]# influx
Connected to http://localhost:8086 version 1.8.10
InfluxDB shell version: 1.8.10
> show databases;
name: databases
name
----
_internal
graphite
test
> DROP DATABASE "graphite";
> show databases;
name: databases
name
----
_internal
test
> quit

```

## Create retention policy as default.

```
> CREATE RETENTION POLICY db_retention_policy_1d ON graphite DURATION 1d REPLICATION 1 default;
> SHOW RETENTION POLICIES
name                   duration shardGroupDuration replicaN default
----                   -------- ------------------ -------- -------
autogen                0s       168h0m0s           1        false
db_retention_policy    48h0m0s  24h0m0s            1        false
db_retention_policy_1d 24h0m0s  1h0m0s             1        true
>
> precision rfc3339
> select * from "autogen"."threadpool.completeTasks" limit 10;
name: threadpool.completeTasks
time                 applicationid                    namespace process username value
----                 -------------                    --------- ------- -------- -----
2023-08-31T09:20:27Z application_1681733404343_224278 executor  1       dev  338
2023-08-31T09:20:27Z application_1681733404343_224278 executor  2       dev  339
2023-08-31T09:20:29Z application_1681733404343_228924 executor  1       dev  9

```

This is to delete old data and apply the correct retention policy and autogen will get disabled.


## The graphite database will automatically get created once any spark job sends the metrics to it.


Backup the database from another environment and restore it to this environment.

#### Backup - 

```
[root@rosdevinfluxdbgrafana01 influxbkp]# mkdir graphite
[root@rosdevinfluxdbgrafana01 influxbkp]# influxd backup -portable -database graphite graphite/

```
#### Restore on the target ( Make sure the backup is zipped and moved to target server) - 

```
[root@server51 influxbkp]# influxd restore -portable -db graphite /root/influxbkp/graphite
```

Reference - https://github.com/cerndb/spark-dashboard/blob/master
Reference - https://stackoverflow.com/questions/27779472/export-data-from-influxdb