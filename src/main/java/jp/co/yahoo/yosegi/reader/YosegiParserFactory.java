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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.message.parser.ISettableIndexParser;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;

import java.util.Objects;

public final class YosegiParserFactory {
  @FunctionalInterface
  private interface HasDispatchedFunc {
    public boolean apply(IColumn column, int index);
  }

  @FunctionalInterface
  private interface GedDispatchedFunc {
    public ISettableIndexParser apply(IColumn column, int index);
  }

  private static EnumDispatcherFactory.Func<ColumnType, HasDispatchedFunc> hasDispatcher;
  private static EnumDispatcherFactory.Func<ColumnType, GedDispatchedFunc> getDispatcher;

  static {
    EnumDispatcherFactory<ColumnType, GedDispatchedFunc> sg =
        new EnumDispatcherFactory<>(ColumnType.class);
    sg.setDefault((column, index) -> YosegiNullParser.getInstance());
    sg.set(ColumnType.SPREAD, (column, index) -> new YosegiSpreadParser(column));
    sg.set(ColumnType.ARRAY,  (column, index) -> new YosegiArrayParser(column));
    sg.set(ColumnType.UNION,  (column, index) -> {
      return get(column.getColumn(column.get(index).getType()), index);
    });
    getDispatcher = sg.create();


    EnumDispatcherFactory<ColumnType, HasDispatchedFunc> sh =
        new EnumDispatcherFactory<ColumnType, HasDispatchedFunc>(ColumnType.class);
    sh.setDefault((column, index) -> false);
    sh.set(ColumnType.SPREAD, (column, index) -> true);
    sh.set(ColumnType.ARRAY,  (column, index) -> true);
    sh.set(ColumnType.UNION,  (column, index) -> {
      return hasParser(column.getColumn(column.get(index).getType()), index);
    });
    hasDispatcher = sh.create();
  }


  private YosegiParserFactory() {}

  /**
   * Convert a column to a parser object.
   */
  public static ISettableIndexParser get(final IColumn column, final int index) {
    if (Objects.isNull(column)) {
      return null;
    }
    return getDispatcher.get(column.getColumnType()).apply(column, index);
  }

  /**
   * Determine whether the target column has a child column.
   */
  public static boolean hasParser(final IColumn column, final int index) {
    if (Objects.isNull(column)) {
      return false;
    }
    return hasDispatcher.get(column.getColumnType()).apply(column, index);
  }
}

