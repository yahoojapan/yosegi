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

package jp.co.yahoo.yosegi.message.parser.text;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;

public class TextParserFactory {
  @FunctionalInterface
  private interface DispatchedFunc {
    public IParser apply(
        final byte[] buffer,
        final int start,
        final int length,
        final IField schema) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory();
    sw.setDefault((buffer, start, length, schema) -> new TextNullParser());
    sw.set(ArrayContainerField.class, (buffer, start, length, schema) ->
        new TextArrayParser(buffer, start, length, (ArrayContainerField)schema));
    sw.set(StructContainerField.class, (buffer, start, length, schema) ->
        new TextStructParser(buffer, start, length, (StructContainerField)schema));
    sw.set(MapContainerField.class, (buffer, start, length, schema) ->
        new TextMapParser(buffer, start, length, (MapContainerField)schema));
    dispatcher = sw.create();
  }


  /**
   * Create an object to parse byte array.
   */
  public static IParser get(
      final byte[] buffer ,
      final int start ,
      final int length ,
      final IField schema ) throws IOException {
    return dispatcher.get(schema.getClass()).apply(buffer, start, length, schema);
  }
}

