package com.scoperetail.fusion.connect.broadcaster.route;

/*-
 * *****
 * hawkeye-heartbeat
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

import java.util.List;
import java.util.Optional;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.core.type.TypeReference;
import com.scoperetail.fusion.connect.broadcaster.common.JsonUtils;
import com.scoperetail.fusion.connect.broadcaster.dto.Event;

@Component
public class CacheRouteBuilder extends RouteBuilder {

  public static final String CACHE_ROUTE = "direct:cache";
  private static final String CACHE_DATA_URL = "cacheDataUrl";

  @Value("${fusion.resourceURL}")
  private String resourceUrl;

  @Override
  public void configure() throws Exception {
    from(CACHE_ROUTE)
        .setHeader(Exchange.HTTP_METHOD, simple("GET"))
        .process(
            new Processor() {
              @Override
              public void process(final Exchange exchange) throws Exception {
                final Optional<String> optCacheDataUrl = Optional.ofNullable(resourceUrl);
                optCacheDataUrl.ifPresent(
                    cacheDataUrl -> {
                      log.info("Fetching event data from:{}", cacheDataUrl);
                      exchange.setProperty(CACHE_DATA_URL, cacheDataUrl);
                    });
              }
            })
        .choice()
        .when(exchangeProperty(CACHE_DATA_URL).isNotNull())
        .toD("${exchangeProperty.cacheDataUrl}")
        .process(
            new Processor() {
              @Override
              public void process(final Exchange exchange) throws Exception {
                final String response = exchange.getIn().getBody(String.class);
                log.info("Cache data response:{}", response);
                final List<Event> events =
                    JsonUtils.unmarshal(
                        Optional.of(response), Optional.of(new TypeReference<List<Event>>() {}));
                exchange.getIn().setBody(events);
              }
            })
        .endChoice();
  }
}
