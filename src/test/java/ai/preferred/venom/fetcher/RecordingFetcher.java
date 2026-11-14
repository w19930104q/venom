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

package ai.preferred.venom.fetcher;

import ai.preferred.venom.request.Request;
import ai.preferred.venom.response.Response;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * A fetcher wrapper that records the URL of every request before delegating
 * to another fetcher. Recording happens on the calling thread, so the
 * recorded order matches the dequeue order when connections are serialised.
 */
public class RecordingFetcher implements Fetcher {

  private final Fetcher delegate;
  private final List<String> fetchedUrls = new CopyOnWriteArrayList<>();

  public RecordingFetcher(final Fetcher delegate) {
    this.delegate = delegate;
  }

  public List<String> getFetchedUrls() {
    return fetchedUrls;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public @NotNull Future<Response> fetch(@NotNull final Request request) {
    return fetch(request, Callback.EMPTY_CALLBACK);
  }

  @Override
  public @NotNull Future<Response> fetch(@NotNull final Request request, @NotNull final Callback callback) {
    fetchedUrls.add(request.getUrl());
    return delegate.fetch(request, callback);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
