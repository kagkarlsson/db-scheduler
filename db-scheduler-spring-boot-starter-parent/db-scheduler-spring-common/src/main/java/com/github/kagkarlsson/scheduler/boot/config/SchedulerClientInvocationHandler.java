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
package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.Scheduler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cglib.proxy.InvocationHandler;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

public class SchedulerClientInvocationHandler implements InvocationHandler {
  private final ObjectProvider<Scheduler> scheduler;

  public SchedulerClientInvocationHandler(ObjectProvider<Scheduler> scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Scheduler schedulerObject = scheduler.getObject();
    if (ReflectionUtils.isEqualsMethod(method)) {
      return schedulerObject == args[0];
    } else if (ReflectionUtils.isHashCodeMethod(method)) {
      return System.identityHashCode(schedulerObject);
    } else if (ReflectionUtils.isToStringMethod(method)) {
      return ObjectUtils.nullSafeToString(schedulerObject);
    }
    try {
      return method.invoke(schedulerObject, args);
    } catch (InvocationTargetException ex) {
      throw ex.getTargetException();
    }
  }
}
