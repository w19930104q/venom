/*
 * Copyright (c) 2021 Preferred.AI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.preferred.venom.job;

import ai.preferred.venom.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class provides a host-fair wrapper around a job queue.
 * <p>
 * When host fairness is enabled, at most one pending job of each host
 * may be "in flight" (dequeued but not yet released). All other jobs of
 * the same host are held back in a per-host queue and interleaved with
 * jobs of other hosts in stable round-robin order, so that a single host
 * producing tasks continuously cannot starve slower hosts indefinitely.
 * Within a host, jobs are ordered by priority; jobs of equal priority
 * keep a stable first-in-first-out order. Among hosts, the highest
 * priority available is served first and hosts sharing that priority
 * alternate in round-robin order.
 * </p>
 * <p>
 * A slot reserved by a dequeued job must be released through
 * {@link #releaseSlot(Job)} once the job has finished, been cancelled or
 * been re-queued for a retry. Releasing the slot is idempotent. Seeds,
 * lazy depth-probing requests and retries flow through the same path as
 * ordinary jobs. When host fairness is disabled, every operation
 * delegates to the underlying queue and the behaviour is identical to
 * using that queue directly.
 * </p>
 *
 * @author Preferred.AI
 */
public final class FairHostJobQueue extends AbstractQueue<Job>
    implements BlockingQueue<Job>, JobSlotManager {

  /**
   * Logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(FairHostJobQueue.class);

  /**
   * The host key used when the host of a request cannot be determined.
   */
  private static final String UNKNOWN_HOST = "";

  /**
   * An object to synchronise upon, all queue state is guarded by this lock.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Signalled whenever a job becomes available or a slot is released.
   */
  private final Condition notEmpty = lock.newCondition();

  /**
   * {@code true} when host fairness is enabled.
   */
  private final boolean fairHostSharing;

  /**
   * The underlying job queue used when host fairness is disabled.
   */
  private final BlockingQueue<Job> delegate;

  /**
   * Per-host state, keyed by host, used when host fairness is enabled.
   */
  private final Map<String, HostState> hostStates = new HashMap<>();

  /**
   * Round-robin order of hosts with pending jobs.
   */
  private final Deque<String> rotation = new ArrayDeque<>();

  /**
   * Hosts whose single slot is currently held by a dequeued job.
   */
  private final Map<Job, String> inflightHosts = new HashMap<>();

  /**
   * Monotonic sequence used to keep equal-priority jobs in FIFO order.
   */
  private final AtomicLong sequence = new AtomicLong();

  /**
   * Total number of jobs waiting in the per-host queues.
   */
  private int queuedCount;

  /**
   * Constructs an instance of FairHostJobQueue with host fairness enabled,
   * backed by a priority queue with the same ordering as
   * {@link PriorityJobQueue}.
   */
  public FairHostJobQueue() {
    this(true);
  }

  /**
   * Constructs an instance of FairHostJobQueue backed by a priority queue
   * with the same ordering as {@link PriorityJobQueue}.
   *
   * @param fairHostSharing {@code true} to enable per-host fair scheduling
   */
  public FairHostJobQueue(final boolean fairHostSharing) {
    this(new PriorityJobQueue(), fairHostSharing);
  }

  /**
   * Constructs an instance of FairHostJobQueue.
   * <p>
   * When host fairness is disabled, the given queue is used directly and
   * this class behaves exactly like the supplied queue. When enabled,
   * per-host ordering still uses the same priority semantics as
   * {@link PriorityJobQueue}.
   * </p>
   *
   * @param delegate        the underlying job queue
   * @param fairHostSharing {@code true} to enable per-host fair scheduling
   */
  public FairHostJobQueue(final BlockingQueue<Job> delegate, final boolean fairHostSharing) {
    this.delegate = delegate;
    this.fairHostSharing = fairHostSharing;
  }

  /**
   * Gets whether host fairness is enabled for this queue.
   *
   * @return {@code true} when per-host fair scheduling is enabled
   */
  public boolean isFairHostSharing() {
    return fairHostSharing;
  }

  /**
   * Resolves the host of a request from its URL. Requests whose host
   * cannot be determined share a single fallback host key.
   *
   * @param request the request to inspect
   * @return the host of the request, or the fallback key when unknown
   */
  private static String hostOf(final Request request) {
    try {
      final String host = new URI(request.getUrl()).getHost();
      if (host != null) {
        return host.toLowerCase();
      }
    } catch (final Exception e) {
      LOGGER.debug("Unable to determine host for url {}, using fallback host.", request.getUrl());
    }
    return UNKNOWN_HOST;
  }

  /**
   * Ensures the job carries a priority attribute, mirroring the behaviour
   * of {@link AbstractPriorityJobQueue}.
   *
   * @param job the job to check
   * @return the input job
   */
  private static Job ensurePriorityJobAttribute(final Job job) {
    if (job.getJobAttribute(PriorityJobAttribute.class) == null) {
      job.setJobAttribute(new PriorityJobAttribute());
    }
    return job;
  }

  private PriorityJobAttribute priorityOf(final Job job) {
    return job.getJobAttribute(PriorityJobAttribute.class);
  }

  @Override
  public boolean offer(final @Nonnull Job job) {
    if (!fairHostSharing) {
      return delegate.offer(job);
    }
    final ReentrantLock currentLock = this.lock;
    currentLock.lock();
    try {
      offerFair(ensurePriorityJobAttribute(job));
      notEmpty.signal();
    } finally {
      currentLock.unlock();
    }
    return true;
  }

  @Override
  public void put(final @Nonnull Job job) throws InterruptedException {
    offer(job);
  }

  @Override
  public boolean offer(final @Nonnull Job job, final long timeout, final @Nonnull TimeUnit unit)
      throws InterruptedException {
    return offer(job);
  }

  /**
   * Enqueues a job into the per-host queue of its host.
   *
   * @param job the job to enqueue, already carrying a priority attribute
   */
  private void offerFair(final Job job) {
    final String host = hostOf(job.getRequest());
    HostState state = hostStates.get(host);
    if (state == null) {
      state = new HostState();
      hostStates.put(host, state);
    }
    if (state.queue.isEmpty()) {
      rotation.addLast(host);
    }
    state.queue.offer(new SequencedJob(job, sequence.getAndIncrement()));
    queuedCount++;
  }

  @Override
  public Job poll() {
    if (!fairHostSharing) {
      return delegate.poll();
    }
    lock.lock();
    try {
      return pollFairLocked();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Job take() throws InterruptedException {
    if (!fairHostSharing) {
      return delegate.take();
    }
    lock.lockInterruptibly();
    try {
      Job job;
      while ((job = pollFairLocked()) == null) {
        notEmpty.await();
      }
      return job;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Job poll(final long timeout, final @Nonnull TimeUnit unit) throws InterruptedException {
    if (!fairHostSharing) {
      return delegate.poll(timeout, unit);
    }
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      Job job;
      while ((job = pollFairLocked()) == null) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
      return job;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Selects the next eligible job under host fairness. The highest
   * priority available among hosts whose slot is free is selected, with
   * stable round-robin alternation between hosts of that priority.
   * <p>
   * Must be called with {@link #lock} held.
   * </p>
   *
   * @return the next job, or {@code null} when no host is eligible
   */
  private Job pollFairLocked() {
    PriorityJobAttribute highest = null;
    final int hosts = rotation.size();
    for (int i = 0; i < hosts; i++) {
      final String host = rotation.pollFirst();
      final HostState state = hostStates.get(host);
      final SequencedJob candidate = state.queue.peek();
      if (candidate == null) {
        continue;
      }
      if (state.inflight) {
        // The host is still processing its single in-flight job; revisit later.
        rotation.addLast(host);
        continue;
      }

      final PriorityJobAttribute candidatePriority = priorityOf(candidate.job);
      if (highest == null || candidatePriority.compareTo(highest) < 0) {
        highest = candidatePriority;
      }
      rotation.addLast(host);
    }

    if (highest == null) {
      return null;
    }

    final int eligibleHosts = rotation.size();
    for (int i = 0; i < eligibleHosts; i++) {
      final String host = rotation.pollFirst();
      final HostState state = hostStates.get(host);
      final SequencedJob head = state.queue.peek();
      if (head == null) {
        continue;
      }
      final boolean eligible = !state.inflight && priorityOf(head.job).compareTo(highest) == 0;
      if (eligible) {
        final SequencedJob selected = state.queue.poll();
        queuedCount--;
        state.inflight = true;
        inflightHosts.put(selected.job, host);
        if (state.queue.isEmpty()) {
          hostStates.remove(host);
        } else {
          rotation.addLast(host);
        }
        return selected.job;
      }
      rotation.addLast(host);
    }
    return null;
  }

  @Override
  public void releaseSlot(final Job job) {
    if (!fairHostSharing || job == null) {
      return;
    }
    lock.lock();
    try {
      final String host = inflightHosts.remove(job);
      if (host == null) {
        return;
      }
      final HostState state = hostStates.get(host);
      if (state == null) {
        return;
      }
      state.inflight = false;
      if (state.queue.isEmpty()) {
        hostStates.remove(host);
        rotation.remove(host);
      }
      // Re-evaluate eligibility even if no job becomes available: other waiters
      // may be able to proceed, and an empty queue must unblock timed pollers.
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Job peek() {
    if (!fairHostSharing) {
      return delegate.peek();
    }
    lock.lock();
    try {
      PriorityJobAttribute highest = null;
      for (final String host : rotation) {
        final HostState state = hostStates.get(host);
        if (state == null || state.inflight) {
          continue;
        }
        final SequencedJob head = state.queue.peek();
        if (head == null) {
          continue;
        }
        final PriorityJobAttribute headPriority = priorityOf(head.job);
        if (highest == null || headPriority.compareTo(highest) < 0) {
          highest = headPriority;
        }
      }
      if (highest == null) {
        return null;
      }
      for (final String host : rotation) {
        final HostState state = hostStates.get(host);
        if (state == null || state.inflight) {
          continue;
        }
        final SequencedJob head = state.queue.peek();
        if (head != null && priorityOf(head.job).compareTo(highest) == 0) {
          return head.job;
        }
      }
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    if (!fairHostSharing) {
      return delegate.size();
    }
    lock.lock();
    try {
      return queuedCount;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    if (!fairHostSharing) {
      return delegate.isEmpty();
    }
    lock.lock();
    try {
      return queuedCount == 0;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int remainingCapacity() {
    if (!fairHostSharing) {
      return delegate.remainingCapacity();
    }
    return Integer.MAX_VALUE;
  }

  @Nonnull
  @Override
  public Iterator<Job> iterator() {
    if (!fairHostSharing) {
      return delegate.iterator();
    }
    lock.lock();
    try {
      final List<Job> snapshot = new ArrayList<>(queuedCount);
      for (final String host : rotation) {
        final HostState state = hostStates.get(host);
        if (state != null) {
          for (final SequencedJob sequenced : state.queue) {
            snapshot.add(sequenced.job);
          }
        }
      }
      return snapshot.iterator();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int drainTo(final @Nonnull Collection<? super Job> collection) {
    return drainTo(collection, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(final @Nonnull Collection<? super Job> collection, final int maxElements) {
    if (!fairHostSharing) {
      return delegate.drainTo(collection, maxElements);
    }
    lock.lock();
    try {
      int drained = 0;
      while (drained < maxElements) {
        final Job job = pollFairLocked();
        if (job == null) {
          break;
        }
        collection.add(job);
        drained++;
      }
      return drained;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    if (!fairHostSharing) {
      delegate.clear();
      return;
    }
    lock.lock();
    try {
      final Iterator<Map.Entry<String, HostState>> states = hostStates.entrySet().iterator();
      while (states.hasNext()) {
        final Map.Entry<String, HostState> entry = states.next();
        final HostState state = entry.getValue();
        state.queue.clear();
        // Hosts with a job still in flight keep their reservation; they must not
        // accept a second in-flight job if new jobs are enqueued afterwards.
        if (!state.inflight) {
          states.remove();
        }
      }
      rotation.clear();
      queuedCount = 0;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean contains(final Object object) {
    if (!fairHostSharing) {
      return delegate.contains(object);
    }
    if (!(object instanceof Job)) {
      return false;
    }
    lock.lock();
    try {
      for (final HostState state : hostStates.values()) {
        for (final SequencedJob sequenced : state.queue) {
          if (sequenced.job == object) {
            return true;
          }
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean remove(final Object object) {
    if (!fairHostSharing) {
      return delegate.remove(object);
    }
    if (!(object instanceof Job)) {
      return false;
    }
    lock.lock();
    try {
      final Iterator<Map.Entry<String, HostState>> states = hostStates.entrySet().iterator();
      while (states.hasNext()) {
        final Map.Entry<String, HostState> entry = states.next();
        final HostState state = entry.getValue();
        final Iterator<SequencedJob> jobs = state.queue.iterator();
        while (jobs.hasNext()) {
          if (jobs.next().job == object) {
            jobs.remove();
            queuedCount--;
            if (state.queue.isEmpty()) {
              states.remove();
              rotation.remove(entry.getKey());
            }
            return true;
          }
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Object[] toArray() {
    lock.lock();
    try {
      return super.toArray();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> T[] toArray(final @Nonnull T[] array) {
    lock.lock();
    try {
      return super.toArray(array);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean containsAll(final @Nonnull Collection<?> collection) {
    lock.lock();
    try {
      return super.containsAll(collection);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean addAll(final @Nonnull Collection<? extends Job> collection) {
    return super.addAll(collection);
  }

  @Override
  public boolean removeAll(final @Nonnull Collection<?> collection) {
    lock.lock();
    try {
      return super.removeAll(collection);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean retainAll(final @Nonnull Collection<?> collection) {
    lock.lock();
    try {
      return super.retainAll(collection);
    } finally {
      lock.unlock();
    }
  }

  /**
   * State held for a single host: its waiting jobs in priority/FIFO
   * order and whether its slot is currently in flight.
   */
  private static final class HostState {

    /**
     * Waiting jobs of this host, ordered by priority and then FIFO sequence.
     */
    private final PriorityBlockingQueue<SequencedJob> queue = new PriorityBlockingQueue<>(11,
        Comparator.comparingInt((SequencedJob sequenced) ->
            sequenced.job.getJobAttribute(PriorityJobAttribute.class).getPriority().ordinal())
            .thenComparingLong(sequenced -> sequenced.sequence));

    /**
     * {@code true} while a job of this host is dequeued and in flight.
     */
    private boolean inflight;
  }

  /**
   * A job paired with the monotonic sequence assigned on insertion.
   */
  private static final class SequencedJob {

    /**
     * The wrapped job.
     */
    private final Job job;

    /**
     * Insertion sequence used as a stable tie-breaker among equal priorities.
     */
    private final long sequence;

    /**
     * Constructs a sequenced job.
     *
     * @param job      the wrapped job
     * @param sequence insertion sequence
     */
    private SequencedJob(final Job job, final long sequence) {
      this.job = job;
      this.sequence = sequence;
    }
  }

}
