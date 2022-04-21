package com.scoperetail.fusion.connect.broadcaster.config;

/*-
 * *****
 * fusion-connect-broadcaster
 * -----
 * Copyright (C) 2018 - 2022 Scope Retail Systems Inc.
 * -----
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =====
 */

import static com.scoperetail.fusion.connect.broadcaster.route.CacheRouteBuilder.CACHE_ROUTE;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import com.scoperetail.fusion.connect.broadcaster.dto.Event;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BroadcastInitializer implements ApplicationListener<ContextRefreshedEvent> {
  private final ProducerTemplate producerTemplate;
  private final BroadcasterConfig broadcasterConfig;

  public BroadcastInitializer(
      final BroadcasterConfig broadcasterConfig, final CamelContext camelContext) {
    this.broadcasterConfig = broadcasterConfig;
    this.producerTemplate = camelContext.createProducerTemplate();
  }

  @Override
  public void onApplicationEvent(final ContextRefreshedEvent event) {
    try {
      buildCache();
    } catch (final Exception e) {
      log.error("Exception occured during initialization: {}", e);
      throw new RuntimeException(e);
    }
  }

  private void buildCache() throws InterruptedException, ExecutionException {
    final CompletableFuture<Object> cacheDataResponse =
        producerTemplate.asyncSendBody(CACHE_ROUTE, null);
    final Object cacheData = cacheDataResponse.get();
    if (Objects.nonNull(cacheData) && cacheData instanceof List) {
      broadcasterConfig.setEvents((List<Event>) cacheData);
    }
  }
}
