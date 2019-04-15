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

import java.util.Objects;
import java.util.stream.Stream;

public class NamePair<T extends Enum<T> & INamePair> {
  private Class<T> enumType;
  private SwitchDispatcherFactory.Func<String, T> shortNameToEnum;
  private SwitchDispatcherFactory.Func<String, T> longNameToEnum;

  /**
   * Create name pair that can be gotten name from each other.
   */
  public NamePair(Class<T> enumType) {
    this.enumType = enumType;
    SwitchDispatcherFactory<String, T> ssw = new SwitchDispatcherFactory<>();
    SwitchDispatcherFactory<String, T> lsw = new SwitchDispatcherFactory<>();

    Stream.of(enumType.getEnumConstants()).forEach(type -> {
      ssw.set(type.getShortName(), type);
      lsw.set(type.getLongName(),  type);
    });
    shortNameToEnum = ssw.create();
    longNameToEnum  = lsw.create();
  }

  /**
   * get long name from short name.
   */
  public String getLongName(final String shortName) {
    T type = shortNameToEnum.get(shortName);
    return Objects.isNull(type) ? shortName : type.getLongName();
  }

  /**
   * get short name from long name.
   */
  public String getShortName(final String longName) {
    T type = longNameToEnum.get(longName);
    return Objects.isNull(type) ? longName : type.getShortName();
  }
}

