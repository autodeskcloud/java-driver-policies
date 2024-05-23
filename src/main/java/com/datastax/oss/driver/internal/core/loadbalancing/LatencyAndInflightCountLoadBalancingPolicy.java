/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The load balancing policy that considers both the latency and the number of in-flight requests
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = LatencyAndInflightCountLoadBalancingPolicy
 *     local-datacenter = datacenter1
 *   }
 * }
 * </pre>
 *
 * <p>See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation requires a local datacenter to be defined,
 * otherwise it will throw an {@link IllegalStateException}. A local datacenter can be supplied
 * either:
 *
 * <ol>
 *   <li>Programmatically with {@link
 *       com.datastax.oss.driver.api.core.session.SessionBuilder#withLocalDatacenter(String)
 *       SessionBuilder#withLocalDatacenter(String)};
 *   <li>Through configuration, by defining the option {@link
 *       DefaultDriverOption#LOAD_BALANCING_LOCAL_DATACENTER
 *       basic.load-balancing-policy.local-datacenter};
 *   <li>Or implicitly, if and only if no explicit contact points were provided: in this case this
 *       implementation will infer the local datacenter from the implicit contact point (localhost).
 * </ol>
 *
 * <p><b>Query plan</b>: This implementation differs from the default policy by maintaining an
 * exponential moving average of the latencies for each node and using this score to exclude the
 * slow replicas. It still reorders the first two replicas in the query plan based on in-flight
 * requests count, just as the default policy does.
 */
@ThreadSafe
public class LatencyAndInflightCountLoadBalancingPolicy extends DefaultLoadBalancingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(LatencyAndInflightCountLoadBalancingPolicy.class);

  protected final LoadingCache<Node, NodeLatencyTracker> latencies;

  private final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(2);

  private final long THRESHOLD_TO_ACCOUNT = 100;

  // Just a copy of 4.18.0 java driver NEWLY_UP_INTERVAL_NANOS
  private static final long NEWLY_UP_INTERVAL = MINUTES.toNanos(1);

  private final boolean avoidSlowReplicas;

  public LatencyAndInflightCountLoadBalancingPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
    CacheLoader<Node, NodeLatencyTracker> cacheLoader =
        new CacheLoader<Node, NodeLatencyTracker>() {
          @Override
          public NodeLatencyTracker load(@NonNull Node node) {
            return new NodeLatencyTracker(THRESHOLD_TO_ACCOUNT);
          }
        };
    latencies = CacheBuilder.newBuilder().weakKeys().build(cacheLoader);
    this.avoidSlowReplicas =
        profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    if (!avoidSlowReplicas) {
      return super.newQueryPlan(request, session);
    }

    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = getLiveNodes().dc(getLocalDatacenter()).toArray();

    Set<Node> allReplicas = getReplicas(request, session);
    int replicaCount = 0; // in currentNodes

    if (!allReplicas.isEmpty()) {

      // Move replicas to the beginning of the plan
      for (int i = 0; i < currentNodes.length; i++) {
        Node node = (Node) currentNodes[i];
        if (allReplicas.contains(node)) {
          ArrayUtils.bubbleUp(currentNodes, i, replicaCount);
          replicaCount++;
        }
      }

      if (replicaCount > 1) {

        shuffleHead(currentNodes, replicaCount);

        if (replicaCount > 2) {

          assert session != null;

          Node newestUpReplica = null;

          long mostRecentUpTimeNanos = -1;
          long now = nanoTime();
          for (int i = 0; i < replicaCount; i++) {
            Node node = (Node) currentNodes[i];
            assert node != null;
            Long upTimeNanos = upTimes.get(node);
            if (upTimeNanos != null
                && now - upTimeNanos - NEWLY_UP_INTERVAL < 0
                && upTimeNanos - mostRecentUpTimeNanos > 0) {
              newestUpReplica = node;
              mostRecentUpTimeNanos = upTimeNanos;
            }
          }

          // When:
          // - there isn't any newly UP replica
          // - there are 3 replicas in total
          // bubble down the slowest replica to the end of the replicas
          // - else if there are more than 3 replicas in total
          // bubble down the slowest 2 replicas in the first 4 replicas out of the first 2 positions
          if (newestUpReplica == null) {
            if (replicaCount == 3) {
              compareAndSwapSlowReplica(currentNodes, 0, 1);
              compareAndSwapSlowReplica(currentNodes, 1, 2);
              compareAndSwapSlowReplica(currentNodes, 0, 1);
            } else {
              for (int i = 0; i < 3; i++) {
                compareAndSwapSlowReplica(currentNodes, i, i + 1);
              }
              for (int i = 0; i < 2; i++) {
                compareAndSwapSlowReplica(currentNodes, i, i + 1);
              }
            }
          }

          // When:
          // - there is a newly UP replica and
          // - the replica in first or second position is the most recent replica marked as UP and
          // - dice roll 1d4 != 1
          else if ((newestUpReplica == currentNodes[0] || newestUpReplica == currentNodes[1])
              && diceRoll1d4() != 1) {

            // Send it to the back of the replicas
            ArrayUtils.bubbleDown(
                currentNodes, newestUpReplica == currentNodes[0] ? 0 : 1, replicaCount - 1);
          }

          // Reorder the first two replicas in the shuffled list based on the number of
          // in-flight requests
          if (getInFlight((Node) currentNodes[0], session)
              > getInFlight((Node) currentNodes[1], session)) {
            ArrayUtils.swap(currentNodes, 0, 1);
          }
        }
      }
    }

    LOG.trace("[{}] Prioritizing {} local replicas", logPrefix, replicaCount);

    // Round-robin the remaining nodes
    ArrayUtils.rotate(
        currentNodes,
        replicaCount,
        currentNodes.length - replicaCount,
        roundRobinAmount.getAndUpdate(INCREMENT));

    QueryPlan plan = currentNodes.length == 0 ? QueryPlan.EMPTY : new SimpleQueryPlan(currentNodes);
    return maybeAddDcFailover(request, plan);
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    latencies.getUnchecked(node).add(latencyNanos);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    latencies.getUnchecked(node).add(latencyNanos);
  }

  private void compareAndSwapSlowReplica(Object[] currentNodes, int i, int j) {
    NodeLatencyTracker tracker1 = latencies.getUnchecked((Node) currentNodes[i]);
    NodeLatencyTracker tracker2 = latencies.getUnchecked((Node) currentNodes[j]);
    if (tracker1 != null && tracker2 != null) {
      NodeLatencyTracker.TimestampedAverage average1 = tracker1.getCurrentAverage();
      NodeLatencyTracker.TimestampedAverage average2 = tracker2.getCurrentAverage();
      if (average1 != null
          && average2 != null
          && average1.getAverage() > average2.getAverage()
          && System.nanoTime() - average1.getTimestamp() < RETRY_PERIOD) {
        ArrayUtils.swap(currentNodes, i, j);
      }
    }
  }
}
