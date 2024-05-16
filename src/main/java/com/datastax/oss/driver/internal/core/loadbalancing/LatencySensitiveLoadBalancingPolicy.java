package com.datastax.oss.driver.internal.core.loadbalancing;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.MandatoryLocalDcHelper;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default load balancing policy implementation.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = DefaultLoadBalancingPolicy
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
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled; if more than 2 replicas are
 * available, they will be ordered from most healthy to least healthy ("Power of 2 choices" or busy
 * node avoidance algorithm). Non-replica nodes will be included in a round-robin fashion. If the
 * local datacenter is defined (see above), query plans will only include local nodes, never remote
 * ones; if it is unspecified however, query plans may contain nodes from different datacenters.
 */
@ThreadSafe
public class LatencySensitiveLoadBalancingPolicy extends DefaultLoadBalancingPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);

    private static final long NEWLY_UP_INTERVAL_NANOS = MINUTES.toNanos(1);
    //  private final long SCALE = TimeUnit.MILLISECONDS.toNanos(100);
    private final long THRESHOLD_TO_ACCOUNT = 100;
    private final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(10);

    protected final Map<Node, Long> upTimes = new ConcurrentHashMap<>();
    private final boolean avoidSlowReplicas;

    protected final ConcurrentMap<Node, NodeLatencyTracker> latencies =
            new ConcurrentHashMap<Node, NodeLatencyTracker>();

    public LatencySensitiveLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
        super(context, profileName);
        this.avoidSlowReplicas =
                profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
    }

    @NonNull
    @Override
    protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
        return new MandatoryLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
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

                    // Test replicas health
                    Node newestUpReplica = null;
                    long mostRecentUpTimeNanos = -1;
                    long now = nanoTime();
                    for (int i = 0; i < replicaCount; i++) {
                        Node node = (Node) currentNodes[i];
                        assert node != null;
                        Long upTimeNanos = upTimes.get(node);
                        if (upTimeNanos != null
                                && now - upTimeNanos - NEWLY_UP_INTERVAL_NANOS < 0
                                && upTimeNanos - mostRecentUpTimeNanos > 0) {
                            newestUpReplica = node;
                            mostRecentUpTimeNanos = upTimeNanos;
                        }
                    }

                    // When:
                    // - there is a newly UP replica and
                    // - the replica in first or second position is the most recent replica marked as UP and
                    // - dice roll 1d4 != 1
                    if ((newestUpReplica == currentNodes[0] || newestUpReplica == currentNodes[1])
                            && diceRoll1d4() != 1) {

                        // Send it to the back of the replicas
                        ArrayUtils.bubbleDown(
                                currentNodes, newestUpReplica == currentNodes[0] ? 0 : 1, replicaCount - 1);
                    }

                    // Reorder the first two replicas in the shuffled list
                    // if the first replica's latency is higher than the second replica's latency and
                    // the first replica's latency is not too old
                    NodeLatencyTracker tracker1 = latencies.get((Node) currentNodes[0]);
                    NodeLatencyTracker tracker2 = latencies.get((Node) currentNodes[1]);
                    if (tracker1 != null && tracker2 != null) {
                        TimestampedAverage average1 = tracker1.getCurrentAverage();
                        TimestampedAverage average2 = tracker2.getCurrentAverage();
                        if (average1 != null
                                && average2 != null
                                && average1.average > average2.average
                                && System.nanoTime() - average1.timestamp < RETRY_PERIOD) {
                            ArrayUtils.swap(currentNodes, 0, 1);
                        }
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
        latencies.putIfAbsent(node, new NodeLatencyTracker(THRESHOLD_TO_ACCOUNT));
        latencies.get(node).add(latencyNanos);
    }

    @Override
    public void onNodeError(
            @NonNull Request request,
            @NonNull Throwable error,
            long latencyNanos,
            @NonNull DriverExecutionProfile executionProfile,
            @NonNull Node node,
            @NonNull String logPrefix) {
        latencies.putIfAbsent(node, new NodeLatencyTracker(THRESHOLD_TO_ACCOUNT));
        // even if it's DriverTimeoutException, we still want to put the timeout latency (5s) in it
        latencies.get(node).add(latencyNanos);
    }

    /**
     * Exposed as a protected method so that it can be accessed by tests
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Exposed as a protected method so that it can be accessed by tests
     */
    protected int diceRoll1d4() {
        return ThreadLocalRandom.current().nextInt(4);
    }

    protected static class TimestampedAverage {

        private final long timestamp;
        protected final long average;
        private final long nbMeasure;

        TimestampedAverage(long timestamp, long average, long nbMeasure) {
            this.timestamp = timestamp;
            this.average = average;
            this.nbMeasure = nbMeasure;
        }
    }

    protected static class NodeLatencyTracker {

        private final long thresholdToAccount;
        //    private final double localScale;
        private final AtomicReference<TimestampedAverage> current =
                new AtomicReference<TimestampedAverage>();

        private final long scale = TimeUnit.MILLISECONDS.toNanos(100);

        NodeLatencyTracker(long thresholdToAccount) {
            //      this.localScale = (double) localScale; // We keep in double since that's how we'll use
            // it.
            this.thresholdToAccount = thresholdToAccount;
        }

        public void add(long newLatencyNanos) {
            TimestampedAverage previous, next;
            do {
                previous = current.get();
                next = computeNextAverage(previous, newLatencyNanos);
            } while (next != null && !current.compareAndSet(previous, next));
        }

        private TimestampedAverage computeNextAverage(
                TimestampedAverage previous, long newLatencyNanos) {

            long currentTimestamp = System.nanoTime();

            long nbMeasure = previous == null ? 1 : previous.nbMeasure + 1;
            if (nbMeasure < thresholdToAccount)
                return new TimestampedAverage(currentTimestamp, -1L, nbMeasure);

            if (previous == null || previous.average < 0)
                return new TimestampedAverage(currentTimestamp, newLatencyNanos, nbMeasure);

            // Note: it's possible for the delay to be 0, in which case newLatencyNanos will basically be
            // discarded. It's fine: nanoTime is precise enough in practice that even if it happens, it
            // will be very rare, and discarding a latency every once in a while is not the end of the
            // world.
            // We do test for negative value, even though in theory that should not happen, because it
            // seems
            // that historically there has been bugs here
            // (https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks)
            // so while this is almost surely not a problem anymore, there's no reason to break the
            // computation
            // if this even happen.
            long delay = currentTimestamp - previous.timestamp;
            if (delay <= 0) return null;

            double scaledDelay = ((double) delay) / scale;
            // Note: We don't use log1p because we it's quite a bit slower and we don't care about the
            // precision (and since we
            // refuse ridiculously big scales, scaledDelay can't be so low that scaledDelay+1 == 1.0 (due
            // to rounding)).
            double prevWeight = Math.log(scaledDelay + 1) / scaledDelay;
            long newAverage =
                    (long) ((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

            return new TimestampedAverage(currentTimestamp, newAverage, nbMeasure);
        }

        public TimestampedAverage getCurrentAverage() {
            return current.get();
        }
    }
}
