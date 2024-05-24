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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NodeLatencyTracker {

  private final AtomicReference<TimestampedAverage> current = new AtomicReference<>();

  private final long scale = TimeUnit.MILLISECONDS.toNanos(100);

  private final long thresholdToAccount;

  NodeLatencyTracker(long thresholdToAccount) {
    this.thresholdToAccount = thresholdToAccount;
  }

  public void add(long newLatencyNanos) {
    TimestampedAverage previous, next;
    do {
      previous = current.get();
      next = computeNextAverage(previous, newLatencyNanos);
    } while (next != null && !current.compareAndSet(previous, next));
  }

  private TimestampedAverage computeNextAverage(TimestampedAverage previous, long newLatencyNanos) {

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
    long newAverage = (long) ((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

    return new TimestampedAverage(currentTimestamp, newAverage, nbMeasure);
  }

  public TimestampedAverage getCurrentAverage() {
    return current.get();
  }

  protected static class TimestampedAverage {

    private final long timestamp;
    private final long average;
    private final long nbMeasure;

    TimestampedAverage(long timestamp, long average, long nbMeasure) {
      this.timestamp = timestamp;
      this.average = average;
      this.nbMeasure = nbMeasure;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public long getAverage() {
      return average;
    }
  }
}
