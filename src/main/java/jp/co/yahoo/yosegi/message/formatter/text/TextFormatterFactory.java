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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;

public class TextFormatterFactory {
  @FunctionalInterface
  private interface DispatchedFunc {
    public ITextFormatter apply(IField schema) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory<>();
    sw.setDefault(sc -> new TextNullFormatter());
    sw.set(ArrayContainerField.class,  sc -> new TextArrayFormatter((ArrayContainerField)sc));
    sw.set(StructContainerField.class, sc -> new TextStructFormatter((StructContainerField)sc));
    sw.set(MapContainerField.class,    sc -> new TextMapFormatter((MapContainerField)sc));
    sw.set(BooleanField.class, sc -> new TextBooleanFormatter());
    sw.set(BytesField.class,   sc -> new TextBytesFormatter());
    sw.set(DoubleField.class,  sc -> new TextDoubleFormatter());
    sw.set(FloatField.class,   sc -> new TextFloatFormatter());
    sw.set(IntegerField.class, sc -> new TextIntegerFormatter());
    sw.set(LongField.class,    sc -> new TextLongFormatter());
    sw.set(ShortField.class,   sc -> new TextShortFormatter());
    sw.set(StringField.class,  sc -> new TextStringFormatter());
    sw.set(NullField.class,    sc -> new TextNullFormatter());
    dispatcher = sw.create();
  }

  /**
   * Create an ITextFormatter from a schema.
   */
  public static ITextFormatter get( final IField schema ) throws IOException {
    return dispatcher.get(schema.getClass()).apply(schema);
  }
}

