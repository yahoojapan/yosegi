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

import jp.co.yahoo.yosegi.message.design.FieldType;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;

import java.io.IOException;

public final class TextPrimitiveConverter {
  @FunctionalInterface
  private interface DispatchedFunc {
    PrimitiveObject apply(PrimitiveObject fromObj) throws IOException;
  }

  private static EnumDispatcherFactory.Func<FieldType, DispatchedFunc> dispatcher;

  static {
    EnumDispatcherFactory<FieldType, DispatchedFunc> sw =
        new EnumDispatcherFactory<>(FieldType.class);
    sw.setDefault(fromObj -> fromObj); // including UNION, ARRAY, MAP, STRUCT, BYTES, NULL
    sw.set(FieldType.BOOLEAN, fromObj -> new BooleanObj(fromObj.getBoolean()));
    sw.set(FieldType.BYTE,    fromObj -> new ByteObj(   fromObj.getByte()));
    sw.set(FieldType.DOUBLE,  fromObj -> new DoubleObj( fromObj.getDouble()));
    sw.set(FieldType.FLOAT,   fromObj -> new FloatObj(  fromObj.getFloat()));
    sw.set(FieldType.INTEGER, fromObj -> new IntegerObj(fromObj.getInt()));
    sw.set(FieldType.LONG,    fromObj -> new LongObj(   fromObj.getLong()));
    sw.set(FieldType.SHORT,   fromObj -> new ShortObj(  fromObj.getShort()));
    sw.set(FieldType.STRING,  fromObj -> new StringObj( fromObj.getString()));
    dispatcher = sw.create();
  }


  private TextPrimitiveConverter() {}

  /**
   * Converts the input PrimitiveObject to a PrimitiveObject of the specified IField type.
   */
  public static PrimitiveObject textObjToPrimitiveObj(
      final IField type ,
      final PrimitiveObject fromObj) throws IOException {
    try {
      return dispatcher.get(type.getFieldType()).apply(fromObj);
    } catch (Exception ex) {
      return NullObj.getInstance();
    }
  }
}

