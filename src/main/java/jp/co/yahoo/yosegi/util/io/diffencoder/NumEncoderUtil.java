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

package jp.co.yahoo.yosegi.util.io.diffencoder;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

import java.io.IOException;

public final class NumEncoderUtil {

  /**
   * Calculate logical data size from column size and type.
   */
  public static int getLogicalSize( final int rows , final ColumnType columnType ) {
    int byteSize = Long.BYTES;
    switch ( columnType ) {
      case BYTE:
        byteSize = Byte.BYTES;
        break;
      case SHORT:
        byteSize = Short.BYTES;
        break;
      case INTEGER:
        byteSize = Integer.BYTES;
        break;
      default:
        byteSize = Long.BYTES;
        break;
    }
    return rows * byteSize;
  }

  /**
   * Determine the type of range of difference between min and max.
   */
  public static ColumnType getDiffColumnType( final long min , final long max ) {
    if ( min < 0 && ( min + Long.MAX_VALUE ) < max ) {
      return null;
    }

    long diff = max - min;
    if ( diff <= NumberToBinaryUtils.LONG_BYTE_MAX_LENGTH ) {
      return ColumnType.BYTE;
    } else if ( diff <= NumberToBinaryUtils.LONG_SHORT_MAX_LENGTH ) {
      return ColumnType.SHORT;
    } else if ( diff <= NumberToBinaryUtils.LONG_INT_MAX_LENGTH ) {
      return ColumnType.INTEGER;
    }

    return ColumnType.LONG;
  }

  /**
   * Create an IBinaryMaker to save from min and max with the smallest byte.
   */
  public static INumEncoder createEncoder( final long min , final long max ) throws IOException {
    ColumnType diffType = getDiffColumnType( min , max );
    if ( diffType == null ) {
      return new LongNumEncoder( min , max );
    }

    if ( min == max ) {
      return new FixedNumEncoder( min , max );
    } 

    if ( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min
        && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ) {
      return new ByteNumEncoder();
    } else if ( diffType == ColumnType.BYTE ) {
      return new DiffLongNumEncoder( min , max );
    } else if ( Short.valueOf( Short.MIN_VALUE ).longValue() <= min
        && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ) {
      return new ShortNumEncoder();
    } else if ( diffType == ColumnType.SHORT ) {
      return new DiffLongNumEncoder( min , max );
    } else if ( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min
        && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ) {
      return new IntegerNumEncoder( (int)min , (int)max );
    } else if ( diffType == ColumnType.INTEGER || diffType == ColumnType.LONG ) {
      return new DiffLongNumEncoder( min , max );
    } else {
      return new LongNumEncoder( min , max );
    }
  }

}
