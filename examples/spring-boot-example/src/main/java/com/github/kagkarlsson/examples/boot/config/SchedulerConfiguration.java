/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Base64;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
public class SchedulerConfiguration {
  private static final Pattern BASIC_AUTH = Pattern.compile("Basic (.+)");
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerConfiguration.class);

  /** Bean defined when a configuration-property in DbSchedulerCustomizer needs to be overridden. */
  @Bean
  DbSchedulerCustomizer customizer() {
    return new DbSchedulerCustomizer() {
      @Override
      public Optional<SchedulerName> schedulerName() {
        return Optional.of(new SchedulerName.Fixed("spring-boot-scheduler-1"));
      }

      @Override
      public Optional<Serializer> serializer() {
        return Optional.of(new JacksonSerializer());
      }
    };
  }

  @Bean
  public FilterRegistrationBean<Filter> authFilter() {
    FilterRegistrationBean<Filter> registrationBean = new FilterRegistrationBean<>();

    registrationBean.setFilter(
        (servletRequest, servletResponse, filterChain) -> {
          if (authenticated((HttpServletRequest) servletRequest)) {
            filterChain.doFilter(servletRequest, servletResponse);
          } else {
            HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
            httpResponse.setStatus(401);
            httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"Db-scheduler UI Admin\"");
          }
        });
    registrationBean.addUrlPatterns("/db-scheduler/*");
    registrationBean.setOrder(1);

    return registrationBean;
  }

  private boolean authenticated(HttpServletRequest httpResponse) {
    String auth = httpResponse.getHeader("Authorization");
    if (auth == null) {
      return false;
    }

    Matcher matcher = BASIC_AUTH.matcher(auth);
    if (!matcher.matches()) {
      return false;
    }

    try {
      String encodedUserAndPassword = matcher.group(1);
      String userAndPasswordString = new String(Base64.getDecoder().decode(encodedUserAndPassword));
      String[] userAndPassword = userAndPasswordString.split(":");

      return userAndPassword[0].equals("admin") && userAndPassword[1].equals("password123");
    } catch (Exception e) {
      LOG.info("Failed to decode basic auth");
      return false;
    }
  }
}
