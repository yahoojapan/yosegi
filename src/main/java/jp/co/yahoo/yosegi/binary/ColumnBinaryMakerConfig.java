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

import jp.co.yahoo.yosegi.binary.maker.DumpArrayColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpBooleanColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpBytesColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpFloatColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsupportedColumnBinaryMaker;
import jp.co.yahoo.yosegi.compressor.CompressionPolicy;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.GzipCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import java.io.IOException;

public class ColumnBinaryMakerConfig {

  public Configuration param = new Configuration();

  public ICompressor compressorClass;

  public IColumnBinaryMaker unionMakerClass;
  public IColumnBinaryMaker arrayMakerClass;
  public IColumnBinaryMaker spreadMakerClass;

  public IColumnBinaryMaker booleanMakerClass;
  public IColumnBinaryMaker byteMakerClass;
  public IColumnBinaryMaker bytesMakerClass;
  public IColumnBinaryMaker doubleMakerClass;
  public IColumnBinaryMaker floatMakerClass;
  public IColumnBinaryMaker integerMakerClass;
  public IColumnBinaryMaker longMakerClass;
  public IColumnBinaryMaker shortMakerClass;
  public IColumnBinaryMaker stringMakerClass;

  public CompressionPolicy compressionPolicy;
  public double allowedRatio;

  /**
   * Initialize with the default value.
   */
  public ColumnBinaryMakerConfig() throws IOException {
    compressorClass = FindCompressor.get( GzipCompressor.class.getName() );

    unionMakerClass = FindColumnBinaryMaker.get( DumpUnionColumnBinaryMaker.class.getName() );
    arrayMakerClass = FindColumnBinaryMaker.get( DumpArrayColumnBinaryMaker.class.getName() );
    spreadMakerClass = FindColumnBinaryMaker.get( DumpSpreadColumnBinaryMaker.class.getName() );

    booleanMakerClass = FindColumnBinaryMaker.get(
        DumpBooleanColumnBinaryMaker.class.getName() );
    byteMakerClass = FindColumnBinaryMaker.get(
        UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() );
    doubleMakerClass = FindColumnBinaryMaker.get(
        UnsafeRangeDumpDoubleColumnBinaryMaker.class.getName() );
    floatMakerClass = FindColumnBinaryMaker.get(
        UnsafeRangeDumpFloatColumnBinaryMaker.class.getName() );
    integerMakerClass = FindColumnBinaryMaker.get(
        UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() );
    longMakerClass = FindColumnBinaryMaker.get(
        UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() );
    shortMakerClass = FindColumnBinaryMaker.get(
        UnsafeOptimizeDumpLongColumnBinaryMaker.class.getName() );

    stringMakerClass = FindColumnBinaryMaker.get(
        UnsafeOptimizeDumpStringColumnBinaryMaker.class.getName() );
    bytesMakerClass = FindColumnBinaryMaker.get( DumpBytesColumnBinaryMaker.class.getName() );

    compressionPolicy = CompressionPolicy.DEFAULT;
    allowedRatio = 1.15d;
  }

  /**
   * Initialize with other Config settings.
   */
  public ColumnBinaryMakerConfig( final ColumnBinaryMakerConfig otherConfig ) {
    this.compressorClass = otherConfig.compressorClass;
    this.unionMakerClass = otherConfig.unionMakerClass;
    this.arrayMakerClass = otherConfig.arrayMakerClass;
    this.spreadMakerClass = otherConfig.spreadMakerClass;
    this.booleanMakerClass = otherConfig.booleanMakerClass;
    this.byteMakerClass = otherConfig.byteMakerClass;
    this.bytesMakerClass = otherConfig.bytesMakerClass;
    this.doubleMakerClass = otherConfig.doubleMakerClass;
    this.floatMakerClass = otherConfig.floatMakerClass;
    this.integerMakerClass = otherConfig.integerMakerClass;
    this.longMakerClass = otherConfig.longMakerClass;
    this.shortMakerClass = otherConfig.shortMakerClass;
    this.stringMakerClass = otherConfig.stringMakerClass;
    this.compressionPolicy = otherConfig.compressionPolicy;
    this.allowedRatio = otherConfig.allowedRatio;
  }

  /**
   * Select IColumnBinaryMaker from column type.
   */
  public IColumnBinaryMaker getColumnMaker( final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
        return unionMakerClass;
      case ARRAY:
        return arrayMakerClass;
      case SPREAD:
        return spreadMakerClass;

      case BOOLEAN:
        return booleanMakerClass;
      case BYTE:
        return byteMakerClass;
      case BYTES:
        return bytesMakerClass;
      case DOUBLE:
        return doubleMakerClass;
      case FLOAT:
        return floatMakerClass;
      case INTEGER:
        return integerMakerClass;
      case LONG:
        return longMakerClass;
      case SHORT:
        return shortMakerClass;
      case STRING:
        return stringMakerClass;

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return new UnsupportedColumnBinaryMaker();
    }
  }

}
