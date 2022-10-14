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

package jp.co.yahoo.yosegi.inmemory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;

import java.io.IOException;

public final class ColumnBinaryTestCase {

  private ColumnBinaryTestCase() {}

  public static String[] bytesClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker"};
  }

  public static String[] stringClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker"};
  }

  public static String[] numberClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker"};
  }

  public static String[] floatClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayFloatColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpFloatColumnBinaryMaker"};
  }

  public static String[] doubleClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker"};
  }

  public static String[] booleanClassNames() throws IOException {
    return new String[]{
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBooleanColumnBinaryMaker",
        "jp.co.yahoo.yosegi.binary.maker.FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker"};
  }

  public static ColumnBinary createColumnBinary(String targetClassName, IColumn column) throws IOException {
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    return maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
  }

  public static ColumnBinary createStringColumnBinaryFromString(String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createStringColumnBinaryFromString(defaultConfig.getColumnMaker(ColumnType.STRING).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createStringColumnBinaryFromString(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.STRING, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.STRING, new StringObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createStringColumnBinaryFromBytes(byte[][] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createStringColumnBinaryFromBytes(defaultConfig.getColumnMaker(ColumnType.STRING).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createStringColumnBinaryFromBytes(String targetClassName, byte[][] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.STRING, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.STRING, new BytesObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createBytesColumnBinaryFromString(String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createBytesColumnBinaryFromString(defaultConfig.getColumnMaker(ColumnType.STRING).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createBytesColumnBinaryFromString(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTES, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.BYTES, new StringObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createBooleanColumnBinaryFromBoolean(boolean[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createBooleanColumnBinaryFromBoolean(defaultConfig.getColumnMaker(ColumnType.BOOLEAN).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createBooleanColumnBinaryFromBoolean(String targetClassName, boolean[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BOOLEAN, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.BOOLEAN, new BooleanObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createByteColumnBinaryFromByte(byte[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createByteColumnBinaryFromByte(defaultConfig.getColumnMaker(ColumnType.BYTE).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createByteColumnBinaryFromByte(String targetClassName, byte[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTE, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.BYTE, new ByteObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createShortColumnBinaryFromShort(short[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createShortColumnBinaryFromShort(defaultConfig.getColumnMaker(ColumnType.SHORT).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createShortColumnBinaryFromShort(String targetClassName, short[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.SHORT, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.SHORT, new ShortObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createIntegerColumnBinaryFromInteger(int[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createIntegerColumnBinaryFromInteger(defaultConfig.getColumnMaker(ColumnType.INTEGER).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createIntegerColumnBinaryFromInteger(String targetClassName, int[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.INTEGER, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.INTEGER, new IntegerObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createLongColumnBinaryFromLong(long[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createLongColumnBinaryFromLong(defaultConfig.getColumnMaker(ColumnType.LONG).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createLongColumnBinaryFromLong(String targetClassName, long[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.LONG, new LongObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createFloatColumnBinaryFromFloat(float[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createFloatColumnBinaryFromFloat(defaultConfig.getColumnMaker(ColumnType.FLOAT).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createFloatColumnBinaryFromFloat(String targetClassName, float[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.FLOAT, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.FLOAT, new FloatObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }

  public static ColumnBinary createDoubleColumnBinaryFromDouble(double[] data, boolean[] isNullArray) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    return createDoubleColumnBinaryFromDouble(defaultConfig.getColumnMaker(ColumnType.DOUBLE).getClass().getName(), data, isNullArray);
  }

  public static ColumnBinary createDoubleColumnBinaryFromDouble(String targetClassName, double[] data, boolean[] isNullArray) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.DOUBLE, "column");
    for (int i = 0; i < data.length; i++) {
      if (!isNullArray[i]) {
        column.add(ColumnType.DOUBLE, new DoubleObj(data[i]), i);
      }
    }
    return createColumnBinary(targetClassName, column);
  }
}
