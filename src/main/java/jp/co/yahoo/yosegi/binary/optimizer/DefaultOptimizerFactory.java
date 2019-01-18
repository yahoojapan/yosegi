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

package jp.co.yahoo.yosegi.binary.optimizer;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import java.io.IOException;

public class DefaultOptimizerFactory implements IOptimizerFactory {

  private IOptimizer booleanOptimizer;
  private IOptimizer byteOptimizer;
  private IOptimizer shortOptimizer;
  private IOptimizer integerOptimizer;
  private IOptimizer longOptimizer;
  private IOptimizer floatOptimizer;
  private IOptimizer doubleOptimizer;
  private IOptimizer stringOptimizer;

  @Override
  public void setup( final Configuration config ) throws IOException {
    booleanOptimizer = new BooleanOptimizer( config );
    byteOptimizer = new ByteOptimizer( config );
    shortOptimizer = new ShortOptimizer( config );
    integerOptimizer = new IntegerOptimizer( config );
    longOptimizer = new LongOptimizer( config );
    floatOptimizer = new FloatOptimizer( config );
    doubleOptimizer = new DoubleOptimizer( config );
    stringOptimizer = new StringOptimizer( config );
  }

  @Override
  public IOptimizer get( final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
      case ARRAY:
      case SPREAD:
        return NullOptimizer.INSTANCE;
      case BOOLEAN:
        return booleanOptimizer;
      case BYTE:
        return byteOptimizer;
      case DOUBLE:
        return doubleOptimizer;
      case FLOAT:
        return floatOptimizer;
      case INTEGER:
        return integerOptimizer;
      case LONG:
        return longOptimizer;
      case SHORT:
        return shortOptimizer;
      case BYTES:
        return NullOptimizer.INSTANCE;
      case STRING:
        return stringOptimizer;
      default:
        return NullOptimizer.INSTANCE;
    }
  }

}
