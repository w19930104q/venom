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

/**
 * This interface allows a job queue to reserve a scheduling slot for a
 * job that has been dequeued and release it once the job is finished,
 * cancelled or re-queued for a retry.
 * <p>
 * A job polled from a slot-managed queue remains the sole in-flight job
 * of its host until {@link #releaseSlot(Job)} is invoked. The caller is
 * responsible for releasing the slot on every terminal path, including
 * cancellation and interruption, so that no slot is leaked. Releasing a
 * slot is idempotent; releasing an unknown slot is a no-op.
 * </p>
 *
 * @author Preferred.AI
 */
public interface JobSlotManager {

  /**
   * Releases the scheduling slot reserved by the given job, allowing the
   * next pending job of the same host to be dequeued.
   * <p>
   * This method has no effect if the job does not currently hold a slot.
   * </p>
   *
   * @param job the job whose slot is to be released
   */
  void releaseSlot(Job job);

}
