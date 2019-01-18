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

package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.constants.PrimitiveByteLength;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.PrimitiveType;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ColumnTypeFactory {

  public static final byte B__UNKNOWN = 0;
  public static final byte B__UNION = 1;
  public static final byte B__ARRAY = 2;
  public static final byte B__SPREAD = 3;
  public static final byte B__BOOLEAN = 4;
  public static final byte B__BYTE = 5;
  public static final byte B__BYTES = 6;
  public static final byte B__DOUBLE = 7;
  public static final byte B__FLOAT = 8;
  public static final byte B__INTEGER = 9;
  public static final byte B__LONG = 10;
  public static final byte B__SHORT = 11;
  public static final byte B__STRING = 12;
  public static final byte B__NULL = 13;
  public static final byte B__EMPTY_ARRAY = 14;
  public static final byte B__EMPTY_SPREAD = 15;

  private ColumnTypeFactory() {}

  /**
   * ColumnType is determined from an object.
   */
  public static ColumnType get( final Object obj ) throws IOException {
    if ( obj instanceof PrimitiveObject ) {
      switch ( ( (PrimitiveObject)obj ).getPrimitiveType() ) {
        case STRING:
          return ColumnType.STRING;
        case INTEGER:
          return ColumnType.INTEGER;
        case LONG:
          return ColumnType.LONG;
        case DOUBLE:
          return ColumnType.DOUBLE;
        case BOOLEAN:
          return ColumnType.BOOLEAN;
        case BYTE:
          return ColumnType.BYTE;
        case BYTES:
          return ColumnType.BYTES;
        case FLOAT:
          return ColumnType.FLOAT;
        case SHORT:
          return ColumnType.SHORT;
        case NULL:
        default:
          return ColumnType.NULL;
      }
    } else if ( obj instanceof Map ) {
      if ( ( (Map)obj ).isEmpty() ) {
        return ColumnType.EMPTY_SPREAD;
      }
      return ColumnType.SPREAD;
    } else if ( obj instanceof List ) {
      if ( ( (List)obj ).isEmpty() ) {
        return ColumnType.EMPTY_ARRAY;
      }
      return ColumnType.ARRAY;
    } else if ( obj instanceof IParser ) {
      IParser parser = (IParser)obj;
      if ( parser.size() == 0 ) {
        return ColumnType.EMPTY_SPREAD;
      }
      if ( parser.isArray() ) {
        return ColumnType.ARRAY;
      }
      return ColumnType.SPREAD;
    }

    if ( obj == null ) {
      return ColumnType.NULL;
    } else {
      throw new IOException( "Unsupported object : " + obj.getClass().getName() );
    }
  }

  /**
   * Gets a byte representing ColumnType.
   */
  public static byte getColumnTypeByte( final ColumnType type ) {
    switch ( type ) {
      case UNION:
        return B__UNION;
      case ARRAY:
        return B__ARRAY;
      case SPREAD:
        return B__SPREAD;

      case BOOLEAN:
        return B__BOOLEAN;
      case BYTE:
        return B__BYTE;
      case BYTES:
        return B__BYTES;
      case DOUBLE:
        return B__DOUBLE;
      case FLOAT:
        return B__FLOAT;
      case INTEGER:
        return B__INTEGER;
      case LONG:
        return B__LONG;
      case SHORT:
        return B__SHORT;
      case STRING:
        return B__STRING;

      case NULL:
        return B__NULL;
      case EMPTY_ARRAY:
        return B__EMPTY_ARRAY;
      case EMPTY_SPREAD:
        return B__EMPTY_SPREAD;
      default:
        return B__UNKNOWN;
    }

  }

  /**
   * Get ColumnType from type byte.
   */
  public static ColumnType getColumnTypeFromByte( final byte typeByte ) {
    switch ( typeByte ) {
      case B__UNION:
        return ColumnType.UNION;
      case B__ARRAY:
        return ColumnType.ARRAY;
      case B__SPREAD:
        return ColumnType.SPREAD;

      case B__BOOLEAN:
        return ColumnType.BOOLEAN;
      case B__BYTE:
        return ColumnType.BYTE;
      case B__BYTES:
        return ColumnType.BYTES;
      case B__DOUBLE:
        return ColumnType.DOUBLE;
      case B__FLOAT:
        return ColumnType.FLOAT;
      case B__INTEGER:
        return ColumnType.INTEGER;
      case B__LONG:
        return ColumnType.LONG;
      case B__SHORT:
        return ColumnType.SHORT;
      case B__STRING:
        return ColumnType.STRING;

      case B__NULL:
        return ColumnType.NULL;
      case B__EMPTY_ARRAY:
        return ColumnType.EMPTY_ARRAY;
      case B__EMPTY_SPREAD:
        return ColumnType.EMPTY_SPREAD;
      default:
        return ColumnType.UNKNOWN;
    }

  }

  /**
   * Get ColumnType from a string representing type.
   */
  public static ColumnType getColumnTypeFromName( final String typeName ) {
    switch ( typeName ) {
      case "UNION":
      case "union":
        return ColumnType.UNION;
      case "ARRAY":
      case "array":
        return ColumnType.ARRAY;
      case "SPREAD":
      case "spread":
        return ColumnType.SPREAD;

      case "BOOLEAN":
      case "boolean":
        return ColumnType.BOOLEAN;
      case "BYTE":
      case "byte":
        return ColumnType.BYTE;
      case "BYTES":
      case "bytes":
        return ColumnType.BYTES;
      case "DOUBLE":
      case "double":
        return ColumnType.DOUBLE;
      case "FLOAT":
      case "float":
        return ColumnType.FLOAT;
      case "INTEGER":
      case "integer":
        return ColumnType.INTEGER;
      case "LONG":
      case "long":
        return ColumnType.LONG;
      case "SHORT":
      case "short":
        return ColumnType.SHORT;
      case "STRING":
      case "string":
        return ColumnType.STRING;

      case "NULL":
      case "null":
        return ColumnType.NULL;
      case "EMPTY_ARRAY":
      case "empty_array":
        return ColumnType.EMPTY_ARRAY;
      case "EMPTY_SPREAD":
      case "empty_spread":
        return ColumnType.EMPTY_SPREAD;
      default:
        return ColumnType.UNKNOWN;
    }

  }

  public static int getColumnTypeToJavaPrimitiveByteSize(
      final ColumnType type , final PrimitiveObject object ) throws IOException {
    return getColumnTypeToPrimitiveByteSize( type , object )
        + PrimitiveByteLength.JAVA_OBJECT_LENGTH;
  }

  /**
   * Calculate byte size from column type and object.
   */
  public static int getColumnTypeToPrimitiveByteSize(
      final ColumnType type , final PrimitiveObject object ) throws IOException {
    switch ( type ) {
      case BOOLEAN:
        return PrimitiveByteLength.BOOLEAN_LENGTH;
      case BYTE:
        return Byte.BYTES;
      case BYTES:
        return Integer.BYTES + object.getObjectSize();
      case DOUBLE:
        return Double.BYTES;
      case FLOAT:
        return Float.BYTES;
      case INTEGER:
        return Integer.BYTES;
      case LONG:
        return Long.BYTES;
      case SHORT:
        return Short.BYTES;
      case STRING:
        return Integer.BYTES + object.getObjectSize();

      default:
        return 0;
    }
  }

}
