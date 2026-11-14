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
import ai.preferred.venom.request.VRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class FairHostJobQueueTest {

  private static final String HOST_A = "https://a.example.com";
  private static final String HOST_B = "https://b.example.com";
  private static final String HOST_C = "https://c.example.com";

  private FairHostJobQueue jobQueue;

  private static Job job(final String url) {
    return new Job(new VRequest(url));
  }

  private static Job job(final String url, final Priority priority) {
    return new Job(new VRequest(url), null, new PriorityJobAttribute(priority));
  }

  private static String hostOf(final Job job) {
    return job.getRequest().getUrl();
  }

  private static String hostKey(final Job job) {
    return java.net.URI.create(job.getRequest().getUrl()).getHost();
  }

  private static List<String> hostsOf(final List<Job> jobs) {
    final List<String> hosts = new ArrayList<>();
    for (final Job job : jobs) {
      hosts.add(hostOf(job));
    }
    return hosts;
  }

  @BeforeEach
  void initEach() {
    jobQueue = new FairHostJobQueue(true);
  }

  @Test
  void testDisabledByDefaultFlag() {
    final FairHostJobQueue disabled = new FairHostJobQueue(false);
    Assertions.assertFalse(disabled.isFairHostSharing());
    Assertions.assertTrue(jobQueue.isFairHostSharing());
  }

  @Test
  void testDisabledDelegatesToUnderlyingQueue() {
    final FairHostJobQueue disabled = new FairHostJobQueue(false);
    final Job first = job(HOST_A, Priority.HIGH);
    final Job second = job(HOST_B, Priority.HIGHEST);
    disabled.add(first);
    disabled.add(second);
    // Without fairness the underlying priority queue ordering is preserved as-is.
    Assertions.assertEquals(second, disabled.poll());
    Assertions.assertEquals(first, disabled.poll());
    Assertions.assertNull(disabled.poll());
  }

  @Test
  void testStableRoundRobinAcrossHosts() {
    final Job a1 = job(HOST_A);
    final Job b1 = job(HOST_B);
    final Job c1 = job(HOST_C);
    final Job a2 = job(HOST_A + "/2");
    final Job b2 = job(HOST_B + "/2");
    jobQueue.add(a1);
    jobQueue.add(b1);
    jobQueue.add(c1);
    jobQueue.add(a2);
    jobQueue.add(b2);

    // First round: one job per host, even though host A and B have backlogs.
    final Job firstA = jobQueue.poll();
    final Job firstB = jobQueue.poll();
    final Job firstC = jobQueue.poll();
    Assertions.assertEquals(
        java.util.Arrays.asList(HOST_A, HOST_B, HOST_C),
        java.util.Arrays.asList(hostOf(firstA), hostOf(firstB), hostOf(firstC))
    );
    // All three hosts now hold a slot; backlogged jobs cannot be dequeued yet.
    Assertions.assertNull(jobQueue.poll());

    jobQueue.releaseSlot(firstA);
    jobQueue.releaseSlot(firstB);
    final Job secondA = jobQueue.poll();
    final Job secondB = jobQueue.poll();
    Assertions.assertEquals(
        java.util.Arrays.asList(HOST_A + "/2", HOST_B + "/2"),
        java.util.Arrays.asList(hostOf(secondA), hostOf(secondB))
    );
    jobQueue.releaseSlot(firstC);
    jobQueue.releaseSlot(secondA);
    jobQueue.releaseSlot(secondB);
    Assertions.assertTrue(jobQueue.isEmpty());
  }

  @Test
  void testHotHostCannotStarveOtherHosts() {
    // A single hot host keeps producing tasks; a slow host must still be served.
    jobQueue.add(job(HOST_A + "/1"));
    jobQueue.add(job(HOST_B + "/1"));
    final Job first = jobQueue.poll();
    Assertions.assertEquals(HOST_A + "/1", hostOf(first));

    jobQueue.add(job(HOST_A + "/2"));
    jobQueue.add(job(HOST_A + "/3"));
    jobQueue.add(job(HOST_A + "/4"));
    // Host A still holds the single slot, so its newer jobs must be held back.
    Assertions.assertEquals(HOST_B + "/1", hostOf(jobQueue.poll()));
    jobQueue.releaseSlot(first);

    // Only host A jobs remain; they are now served one at a time, each needing release.
    final Job a2 = jobQueue.poll();
    Assertions.assertEquals(HOST_A + "/2", hostOf(a2));
    Assertions.assertNull(jobQueue.poll());
    jobQueue.releaseSlot(a2);
    Assertions.assertEquals(HOST_A + "/3", hostOf(jobQueue.poll()));
  }

  @Test
  void testOneInFlightJobPerHost() {
    jobQueue.add(job(HOST_A + "/1"));
    jobQueue.add(job(HOST_A + "/2"));
    jobQueue.add(job(HOST_B + "/1"));

    final Job a1 = jobQueue.poll();
    Assertions.assertEquals(HOST_A + "/1", hostOf(a1));
    final Job b1 = jobQueue.poll();
    Assertions.assertEquals(HOST_B + "/1", hostOf(b1));
    // Both hosts are in flight; nothing more can be dequeued despite queued jobs.
    Assertions.assertNull(jobQueue.poll());
    Assertions.assertEquals(1, jobQueue.size());

    jobQueue.releaseSlot(b1);
    Assertions.assertNull(jobQueue.poll(), "host A still in flight and host B has no jobs");
    jobQueue.releaseSlot(a1);
    Assertions.assertEquals(HOST_A + "/2", hostOf(jobQueue.poll()));
  }

  @Test
  void testHighestPriorityServedFirstAcrossHosts() {
    final Job aNormal = job(HOST_A, Priority.NORMAL);
    final Job bHigh = job(HOST_B, Priority.HIGH);
    final Job cNormal = job(HOST_C, Priority.NORMAL);
    jobQueue.add(aNormal);
    jobQueue.add(bHigh);
    jobQueue.add(cNormal);

    Assertions.assertEquals(HOST_B, hostOf(jobQueue.poll()));
    jobQueue.releaseSlot(bHigh);
    // Remaining jobs share a priority and alternate in stable rotation order.
    Assertions.assertEquals(HOST_C, hostOf(jobQueue.poll()));
    Assertions.assertEquals(HOST_A, hostOf(jobQueue.poll()));
  }

  @Test
  void testStableFifoWithinSameHostAndPriority() {
    final Job low1 = job(HOST_A, Priority.LOW);
    final Job normal1 = job(HOST_A, Priority.NORMAL);
    final Job normal2 = job(HOST_A + "/x", Priority.NORMAL);
    final Job low2 = job(HOST_A + "/y", Priority.LOW);
    jobQueue.add(low1);
    jobQueue.add(normal1);
    jobQueue.add(normal2);
    jobQueue.add(low2);

    final Job first = jobQueue.poll();
    Assertions.assertSame(normal1, first);
    jobQueue.releaseSlot(first);
    final Job second = jobQueue.poll();
    Assertions.assertSame(normal2, second);
    jobQueue.releaseSlot(second);
    final Job third = jobQueue.poll();
    Assertions.assertSame(low1, third);
    jobQueue.releaseSlot(third);
    final Job fourth = jobQueue.poll();
    Assertions.assertSame(low2, fourth);
  }

  @Test
  void testRetryRequeuesWithDowngradedPriorityAndReleasesSlot() {
    final Job job = job(HOST_A, Priority.HIGH);
    jobQueue.add(job);
    final Job polled = jobQueue.poll();
    Assertions.assertSame(job, polled);
    Assertions.assertNull(jobQueue.poll(), "slot is held while the job is in flight");

    polled.prepareRetry();
    jobQueue.releaseSlot(polled);
    jobQueue.add(polled);

    final Job retried = jobQueue.poll();
    Assertions.assertSame(job, retried);
    Assertions.assertEquals(Priority.NORMAL,
        retried.getJobAttribute(PriorityJobAttribute.class).getPriority());
    Assertions.assertEquals(2, retried.getTryCount());
    jobQueue.releaseSlot(retried);
    Assertions.assertNull(jobQueue.poll());
  }

  @Test
  void testReleaseSlotIsIdempotentAndUnknownIsNoOp() {
    jobQueue.add(job(HOST_A));
    final Job job = jobQueue.poll();
    jobQueue.releaseSlot(job);
    jobQueue.releaseSlot(job);
    jobQueue.releaseSlot(job(HOST_B));
    Assertions.assertTrue(jobQueue.isEmpty());
  }

  @Test
  void testClearReleasesNoSlotsAndKeepsInFlightAccounting() {
    final Job a1 = job(HOST_A + "/1");
    jobQueue.add(a1);
    jobQueue.add(job(HOST_A + "/2"));
    jobQueue.add(job(HOST_B + "/1"));
    final Job inFlight = jobQueue.poll();

    jobQueue.clear();
    Assertions.assertTrue(jobQueue.isEmpty());
    Assertions.assertEquals(0, jobQueue.size());
    Assertions.assertNull(jobQueue.peek());
    // The in-flight job still owns its slot; clearing must not resurrect queued jobs.
    Assertions.assertNull(jobQueue.poll());

    // New jobs of the in-flight host must wait until the slot is released.
    final Job aNew = job(HOST_A + "/new");
    jobQueue.add(aNew);
    Assertions.assertNull(jobQueue.poll(), "host A slot is still held by the in-flight job");
    jobQueue.releaseSlot(inFlight);
    Assertions.assertSame(aNew, jobQueue.poll());
    jobQueue.releaseSlot(aNew);
    Assertions.assertTrue(jobQueue.isEmpty());
  }

  @Test
  void testDrainToHonoursHostSlots() {
    jobQueue.add(job(HOST_A + "/1"));
    jobQueue.add(job(HOST_A + "/2"));
    jobQueue.add(job(HOST_B + "/1"));
    jobQueue.add(job(HOST_C + "/1"));

    final List<Job> drained = new ArrayList<>();
    final int count = jobQueue.drainTo(drained);
    Assertions.assertEquals(3, count);
    Assertions.assertEquals(
        java.util.Arrays.asList(HOST_A + "/1", HOST_B + "/1", HOST_C + "/1"),
        hostsOf(drained)
    );
    Assertions.assertEquals(1, jobQueue.size());
  }

  @Test
  void testPeekDoesNotRotateOrReserveSlot() {
    final Job a = job(HOST_A, Priority.HIGH);
    final Job b = job(HOST_B);
    jobQueue.add(a);
    jobQueue.add(b);

    Assertions.assertSame(a, jobQueue.peek());
    Assertions.assertSame(a, jobQueue.peek(), "peek must be stable and not dequeue");
    Assertions.assertSame(a, jobQueue.poll());
    Assertions.assertSame(b, jobQueue.peek());
    jobQueue.releaseSlot(a);
    Assertions.assertSame(b, jobQueue.poll());
  }

  @Test
  void testPollTimeoutReturnsNullWhenAllHostsInFlight() throws InterruptedException {
    jobQueue.add(job(HOST_A));
    final Job inFlight = jobQueue.poll();
    final long start = System.nanoTime();
    Assertions.assertNull(jobQueue.poll(50, TimeUnit.MILLISECONDS));
    Assertions.assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(40));
    jobQueue.releaseSlot(inFlight);
  }

  @Test
  void testTakeBlocksUntilSlotReleased() throws InterruptedException {
    jobQueue.add(job(HOST_A + "/1"));
    jobQueue.add(job(HOST_A + "/2"));
    final Job first = jobQueue.poll();
    final AtomicReference<Job> taken = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);
    final Thread taker = new Thread(() -> {
      try {
        started.countDown();
        taken.set(jobQueue.take());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    taker.setDaemon(true);
    taker.start();
    Assertions.assertTrue(started.await(1, TimeUnit.SECONDS));
    Assertions.assertNull(taken.get());

    jobQueue.releaseSlot(first);
    taker.join(2000);
    Assertions.assertFalse(taker.isAlive());
    Assertions.assertEquals(HOST_A + "/2", hostOf(taken.get()));
    jobQueue.releaseSlot(taken.get());
  }

  @Test
  void testTakeWakesOnNewOfferOfAnotherHost() throws InterruptedException {
    jobQueue.add(job(HOST_A));
    final Job inFlight = jobQueue.poll();

    final AtomicReference<Job> taken = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);
    final Thread taker = new Thread(() -> {
      try {
        started.countDown();
        taken.set(jobQueue.take());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    taker.setDaemon(true);
    taker.start();
    Assertions.assertTrue(started.await(1, TimeUnit.SECONDS));
    Assertions.assertNull(taken.get());

    // A job of another host is eligible immediately even though host A is busy.
    jobQueue.add(job(HOST_B));
    taker.join(2000);
    Assertions.assertFalse(taker.isAlive());
    Assertions.assertEquals(HOST_B, hostOf(taken.get()));
    jobQueue.releaseSlot(inFlight);
    jobQueue.releaseSlot(taken.get());
  }

  @Test
  void testPollInterruptible() throws InterruptedException {
    final Thread current = Thread.currentThread();
    final Thread interrupter = new Thread(() -> {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      current.interrupt();
    });
    interrupter.setDaemon(true);
    interrupter.start();
    Assertions.assertThrows(InterruptedException.class,
        () -> jobQueue.poll(5, TimeUnit.SECONDS));
  }

  @Test
  void testMalformedUrlFallsBackToSharedHost() {
    final Job malformed1 = job("not a valid url");
    final Job malformed2 = job("also-not-valid");
    jobQueue.add(malformed1);
    jobQueue.add(malformed2);

    final Job first = jobQueue.poll();
    Assertions.assertSame(malformed1, first);
    Assertions.assertNull(jobQueue.poll(), "fallback host also obeys the single in-flight rule");
    jobQueue.releaseSlot(first);
    Assertions.assertSame(malformed2, jobQueue.poll());
  }

  @Test
  void testSameRequestProcessedOnlyOnce() {
    final Job job = job(HOST_A);
    Assertions.assertTrue(jobQueue.offer(job));
    Assertions.assertFalse(jobQueue.contains(new Job(new VRequest(HOST_A))));
    Assertions.assertTrue(jobQueue.contains(job));
    Assertions.assertEquals(1, jobQueue.size());
    Assertions.assertSame(job, jobQueue.poll());
    Assertions.assertFalse(jobQueue.contains(job));
  }

  @Test
  void testRetriedJobReEnqueuedOnceAndNotDuplicated() {
    final Job job = job(HOST_A);
    jobQueue.add(job);
    final Job first = jobQueue.poll();
    Assertions.assertSame(job, first);

    job.prepareRetry();
    jobQueue.releaseSlot(first);
    jobQueue.add(first);
    Assertions.assertEquals(1, jobQueue.size());

    final Job again = jobQueue.poll();
    Assertions.assertSame(job, again);
    Assertions.assertEquals(2, again.getTryCount());
    // The single in-flight slot prevents a second copy from being dispatched.
    Assertions.assertNull(jobQueue.poll());
    jobQueue.releaseSlot(again);
    Assertions.assertNull(jobQueue.poll());
  }

  @Test
  void testNewJobOfReleasedHostBecomesEligible() {
    jobQueue.add(job(HOST_A + "/1"));
    final Job first = jobQueue.poll();
    Assertions.assertNull(jobQueue.poll());

    jobQueue.releaseSlot(first);
    jobQueue.add(job(HOST_A + "/2"));
    Assertions.assertEquals(HOST_A + "/2", hostOf(jobQueue.poll()));
  }

  @Test
  void testEmptyQueueAfterAllSlotsReleased() {
    jobQueue.add(job(HOST_A));
    jobQueue.add(job(HOST_B));
    final Job a = jobQueue.poll();
    final Job b = jobQueue.poll();
    Assertions.assertTrue(jobQueue.isEmpty());
    jobQueue.releaseSlot(a);
    jobQueue.releaseSlot(b);
    Assertions.assertTrue(jobQueue.isEmpty());
    Assertions.assertEquals(0, jobQueue.size());
    Assertions.assertNull(jobQueue.peek());
  }

  @Test
  void testRemoveAndIteratorReflectFairQueues() {
    final Job a1 = job(HOST_A + "/1");
    final Job a2 = job(HOST_A + "/2");
    final Job b1 = job(HOST_B + "/1");
    jobQueue.add(a1);
    jobQueue.add(a2);
    jobQueue.add(b1);

    final Set<Job> snapshot = new HashSet<>();
    jobQueue.iterator().forEachRemaining(snapshot::add);
    Assertions.assertEquals(3, snapshot.size());
    Assertions.assertTrue(snapshot.contains(a2));

    Assertions.assertTrue(jobQueue.remove(a2));
    Assertions.assertEquals(2, jobQueue.size());
    Assertions.assertFalse(jobQueue.contains(a2));
    Assertions.assertFalse(jobQueue.remove(a2));
  }

  @Test
  void testFixedSyncPointsNoHostExceedsOneInFlight() throws InterruptedException {
    // Fixed synchronisation points: producer hands off jobs, consumers release slots.
    final int hosts = 4;
    final int perHost = 25;
    for (int h = 0; h < hosts; h++) {
      for (int i = 0; i < perHost; i++) {
        jobQueue.add(job("https://h" + h + ".example.com/" + i));
      }
    }

    final int consumers = 4;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(consumers);
    final Map<String, Integer> inFlight = new ConcurrentHashMap<>();
    final Map<String, Integer> seen = new ConcurrentHashMap<>();
    final List<String> violations = Collections.synchronizedList(new ArrayList<>());

    for (int c = 0; c < consumers; c++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
          Job job;
          while ((job = jobQueue.poll(200, TimeUnit.MILLISECONDS)) != null) {
            final String host = hostKey(job);
            final int active = inFlight.merge(host, 1, Integer::sum);
            if (active > 1) {
              violations.add(host + " had " + active + " in-flight jobs");
            }
            // Fixed sync point: the slot remains held until the release handshake.
            Thread.yield();
            inFlight.merge(host, -1, Integer::sum);
            seen.merge(host, 1, Integer::sum);
            jobQueue.releaseSlot(job);
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      worker.setDaemon(true);
      worker.start();
    }

    start.countDown();
    Assertions.assertTrue(done.await(30, TimeUnit.SECONDS));
    Assertions.assertTrue(violations.isEmpty(), violations::toString);
    Assertions.assertEquals(hosts, seen.size());
    for (int h = 0; h < hosts; h++) {
      Assertions.assertEquals(perHost, seen.get("h" + h + ".example.com"));
      Assertions.assertEquals(0, inFlight.getOrDefault("h" + h + ".example.com", 0));
    }
    Assertions.assertTrue(jobQueue.isEmpty());
    Assertions.assertEquals(0, jobQueue.size());
  }

  @Test
  void testLazyQueueAdaptationUnchangedWhenDisabled() {
    final List<Request> requests = new ArrayList<>();
    requests.add(new VRequest(HOST_A));
    requests.add(new VRequest(HOST_B));
    final LazyPriorityJobQueue lazy = new LazyPriorityJobQueue(requests.iterator());
    final FairHostJobQueue adapted = new FairHostJobQueue(lazy, false);
    Assertions.assertFalse(adapted.isFairHostSharing());
    Assertions.assertEquals(HOST_A, adapted.poll().getRequest().getUrl());
    Assertions.assertEquals(HOST_B, adapted.poll().getRequest().getUrl());
  }

}
