# Complimentary Load Balancing Policies for Apache Cassandra Java Driver
This package contains two load balancing policies to be used with the [Apache Cassandra Java Driver](https://github.com/apache/cassandra-java-driver) (version 4.10.0+) - `LatencyAndInflightCountLoadBalancingPolicy` and `LatencySensitiveLoadBalancingPolicy`.

## Get Started
To use either of the policies, you need to include both the Java driver and this package as dependencies.
```xml
        <dependency>
            <groupId>org.apache.cassandra</groupId>
            <artifactId>java-driver-core</artifactId>
            <version>4.10.0</version>
        </dependency>
        <dependency>
            <groupId>com.datastax.oss</groupId>
            <artifactId>driver-load-balancing-policies</artifactId>
            <version>1.0</version>
        </dependency>
```

You also have to specify the name of the load balancing policy class in your `application.conf` (see [this](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/load_balancing/index.html#load-balancing)).
```conf
datastax-java-driver.basic.load-balancing-policy {
  class = LatencySensitiveLoadBalancingPolicy
}
```

## How to Choose a Load Balancing Policy
We recommend the `DefaultLoadBalancingPolicy` that comes with the Java Driver for general use. 
This policy leverages real-time measurements and swiftly responds to changes in node status at short intervals, such as those caused by garbage collection or compaction—common factors that can slow down nodes. 
However, if you anticipate prolonged delays in node responsiveness, such as during network upgrades or heavy data migrations, you might consider opting for the `LatencyAndInflightCountLoadBalancingPolicy` or `LatencySensitiveLoadBalancingPolicy`.
