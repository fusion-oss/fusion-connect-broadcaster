package com.scoperetail.fusion.connect.broadcaster.route;

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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import com.scoperetail.fusion.connect.broadcaster.config.BroadcasterConfig;
import com.scoperetail.fusion.connect.broadcaster.dto.Event;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BroadcasteRouteBuilder extends RouteBuilder {

  private static final String MESSAGE_ID = "messageId";
  private static final String CORRELATION_ID = "correlationId";
  private static final String EVENT_TYPE = "eventType";
  private static final String TARGET_URI = "targetUri";
  private static final String COMMA = ",";
  private static final String EVENT_COUNT = "EventCount";
  private static final String TARGET_NAME = "<<TARGET_NAME>>";

  @Value("${fusion.broadcaster.in.channel}")
  private String inChannel;

  @Value("${fusion.broadcaster.out.channel}")
  private String outChannel;

  @Value("${targetHeaderBlacklist:}")
  private String targetHeaderBlacklist;

  @Autowired private BroadcasterConfig broadcasterConfig;

  @Override
  public void configure() throws Exception {
    from(inChannel)
        .process(
            new Processor() {
              @Override
              public void process(final Exchange exchange) throws Exception {
                exchange.setProperty(EVENT_COUNT, broadcasterConfig.getEvents().size());
              }
            })
        .loop(exchangeProperty(EVENT_COUNT))
        .process(
            new Processor() {
              @Override
              public void process(final Exchange exchange) throws Exception {
                final Integer loopIndex = (Integer) exchange.getProperty(Exchange.LOOP_INDEX);
                final Event event = broadcasterConfig.getEvents().get(loopIndex);
                updateExchangeHeaders(exchange, event);
                exchange.setProperty(TARGET_URI, buildTargetUri(event));
                blacklistTargetHeaders(exchange);
                if (StringUtils.hasText(event.getPayloadOverride())) {
                  exchange.getIn().setBody(event.getPayloadOverride());
                }
              }
            })
        .toD("${exchangeProperty.targetUri}")
        .end();
  }

  private void updateExchangeHeaders(final Exchange exchange, final Event event) {
    final Map<String, Object> headers = exchange.getIn().getHeaders();
    headers.put(EVENT_TYPE, event.getEventName());
    headers.put(CORRELATION_ID, UUID.randomUUID().toString());
    headers.put(MESSAGE_ID, UUID.randomUUID().toString());
  }

  private String buildTargetUri(final Event event) {
    return outChannel.replace(TARGET_NAME, event.getTopic());
  }

  private void blacklistTargetHeaders(final Exchange exchange) {
    if (StringUtils.hasLength(targetHeaderBlacklist)) {
      Arrays.stream(targetHeaderBlacklist.split(COMMA))
          .forEach(
              s -> {
                exchange.getIn().removeHeader(s);
                exchange.getIn().removeHeaders(s);
              });
      log.debug("Blacklisted target headers: {}", targetHeaderBlacklist);
    }
  }
}
