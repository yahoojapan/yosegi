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

package jp.co.yahoo.yosegi.util;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * RangeCategoryFactory is an implementation of dispatcher of range category.
 */
public class RangeCategoryFactory<V,C> {
  @FunctionalInterface
  public interface Func<V,C> {
    public C get(V value);
  }

  private final TreeMap<V,C> map = new TreeMap<V,C>();
  private C upperCategory = null;

  public RangeCategoryFactory setLower(final V value, final C category) {
    map.put(value, category);
    return this;
  }

  public RangeCategoryFactory setUpper(final C category) {
    upperCategory = category;
    return this;
  }

  /**
  * create dispatcher function.
  */
  public Func create() {
    return new Func<V,C>() {
      @Override
      public C get(V value) {
        Map.Entry<V,C> entry = map.ceilingEntry((V)value);
        return Objects.isNull(entry) ? upperCategory : entry.getValue();
      }
    };
  }
}

