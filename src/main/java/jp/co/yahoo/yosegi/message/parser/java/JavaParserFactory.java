/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.parser.java;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.util.List;
import java.util.Map;

public final class JavaParserFactory {
  @FunctionalInterface
  private interface DispatchedFunc {
    IParser apply(Object obj);
  }

  private static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory<>();
    sw.setDefault(obj -> new JavaNullParser());
    sw.set(List.class, obj -> new JavaListParser((List)obj));
    sw.set(Map.class,  obj -> new JavaMapParser((Map)obj));
    dispatcher = sw.create();
  }

  private JavaParserFactory() {}

  /**
   * Create IParser from Java objects.
   */
  public static IParser get(final Object obj) {
    return dispatcher.get(obj.getClass()).apply(obj);
  }
}

