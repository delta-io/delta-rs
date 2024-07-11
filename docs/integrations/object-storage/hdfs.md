# HDFS Storage Backend
HDFS support is provided via the [hdfs-native-object-store](https://github.com/datafusion-contrib/hdfs-native-object-store) package, which sits on top of [hdfs-native](https://github.com/Kimahriman/hdfs-native). This is an HDFS client written from scratch in Rust, with no bindings to libhdfs or any use of Java. While it supports most common cluster configurations, it does not support every possible client configuration that could exist.

## Supported Configurations
By default, the client looks for existing Hadoop configs in following manner:

- If the `HADOOP_CONF_DIR` environment variable is defined, load configs from `$HADOOP_CONF_DIR/core-site.xml` and `$HADOOP_CONF_DIR/hdfs-site.xml`
- Otherwise, if the `HADOOP_HOME` environment variable is set, load configs from `$HADOOP_HOME/etc/hadoop/core-site.xml` and `$HADOOP_HOME/etc/hadoop/hdfs-site.xml`

Additionally, you can pass Hadoop configs as `storage_options` and these will take precedence over the above configs.

Currently the supported client configuration parameters are:

- `dfs.ha.namenodes.*` - name service support
- `dfs.namenode.rpc-address.*` - name service support
- `fs.viewfs.mounttable.*.link.*` - ViewFS links
- `fs.viewfs.mounttable.*.linkFallback` - ViewFS link fallback

If you find your setup is not supported, please file an issue in the [hdfs-native](https://github.com/Kimahriman/hdfs-native) repository.

## Secure Clusters
The client supports connecting to secure clusters through both Kerberos authentication as well as token authentication, and all SASL protection types are supported. The highest supported protection mechanism advertised by the server will be used.

### Kerberos Support
Kerberos is supported through dynamically loading the `libgssapi_krb5` library. This must be installed separately through your package manager, and currently only works on Linux and Mac.

Debian-based systems:
```bash
apt-get install libgssapi-krb5-2
```

RHEL-based systems:
```bash
yum install krb5-libs
```

MacOS:
```bash
brew install krb5
```

Then simply `kinit` to get your TGT and authentication to HDFS should just work.

### Token Support
Token authentication is supported by looking for a token file located at the environment variable `HADOOP_TOKEN_FILE_LOCATION`. This is the location systems like YARN will automatically place a delegation token, so things will just work inside of YARN jobs.

## Issues
If you face any HDFS-specific issues, please report to the [hdfs-native-object-store](https://github.com/datafusion-contrib/hdfs-native-object-store) repository.