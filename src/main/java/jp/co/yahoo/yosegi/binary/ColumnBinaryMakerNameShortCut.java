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

package jp.co.yahoo.yosegi.binary;

import jp.co.yahoo.yosegi.binary.maker.ConstantColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker;
import jp.co.yahoo.yosegi.util.Pair;

public final class ColumnBinaryMakerNameShortCut {

  private static final Pair CLASS_NAME_PAIR = new Pair();

  static {
    CLASS_NAME_PAIR.set( DumpSpreadColumnBinaryMaker.class.getName()  , "D9" );
    CLASS_NAME_PAIR.set( DumpUnionColumnBinaryMaker.class.getName()   , "D11" );

    CLASS_NAME_PAIR.set( ConstantColumnBinaryMaker.class.getName()   , "C0" );

    // The following are legacy classes.
    // These classes require a legacy jar.
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DumpArrayColumnBinaryMaker"  , "D0" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DumpBooleanColumnBinaryMaker"  , "D1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DumpBytesColumnBinaryMaker"  , "D3" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DumpDoubleColumnBinaryMaker"  , "D4" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DumpFloatColumnBinaryMaker"   , "D5" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpFloatColumnBinaryMaker"   , "XD1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker"   , "XD2" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker"   , "XO0" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeFloatColumnBinaryMaker"   , "XO1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDoubleColumnBinaryMaker"   , "XO2" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker"   , "XO11" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker"   , "XOD10" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker"   , "XOD11" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.RangeDumpDoubleColumnBinaryMaker" , "RD0" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.RangeDumpFloatColumnBinaryMaker"  , "RD5" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeLongColumnBinaryMaker"   , "OD0" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeLongColumnBinaryMaker"   , "O0" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeFloatColumnBinaryMaker"  , "O1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeDoubleColumnBinaryMaker" , "O2" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeStringColumnBinaryMaker" , "O11" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeDumpLongColumnBinaryMaker"   , "OD10" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeDumpStringColumnBinaryMaker" , "OD11" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizeIndexDumpStringColumnBinaryMaker" , "OI11" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.MaxLengthBasedArrayColumnBinaryMaker" , "ML0" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpFloatColumnBinaryMaker" , "ND1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker" , "ND2" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" , "ND3" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker" , "ND4" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBooleanColumnBinaryMaker" , "ND5" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker" , "ND6" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker",
        "ND7");

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayFloatColumnBinaryMaker" , "N1" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker" , "N2" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" , "N3" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker" , "N4" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" , "RLE3" );
    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker" , "RLE4" );

    CLASS_NAME_PAIR.set(
        "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker" , "DRLE4" );

  }

  private ColumnBinaryMakerNameShortCut() {}

  /**
   * Register the shortcut name.
   */
  public static void register( final String className , final String shortCutName ) {
    if ( getClassName( shortCutName ) != null ) {
      throw new RuntimeException( "It is already registered. " + shortCutName );
    }
    if ( getShortCutName( className ) != null ) {
      throw new RuntimeException( "It is already registered. " + className );
    }
    CLASS_NAME_PAIR.set( className , shortCutName );
  }

  /**
   * Get shortcut name from class name.
   */
  public static String getShortCutName( final String className ) {
    String shortCutName = CLASS_NAME_PAIR.getPair2( className );
    if ( shortCutName == null ) {
      return className;
    }
    return shortCutName;
  }

  /**
   * Get class name from shortcut name.
   */
  public static String getClassName( final String shortCutName ) {
    String className = CLASS_NAME_PAIR.getPair1( shortCutName );
    if ( className == null ) {
      return shortCutName;
    }
    return className;
  }

}
