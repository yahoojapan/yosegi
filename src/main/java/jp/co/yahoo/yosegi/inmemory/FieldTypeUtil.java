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

import jp.co.yahoo.yosegi.message.design.FieldType;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import java.io.IOException;

public final class FieldTypeUtil {

  /**
   * column type to field type.
   */
  public static FieldType columnTypeToFieldType( final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
        return FieldType.UNION;
      case ARRAY:
        return FieldType.ARRAY;
      case MAP:
        return FieldType.MAP;
      case STRUCT:
        return FieldType.STRUCT;
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case BYTE:
        return FieldType.BYTE;
      case BYTES:
        return FieldType.BYTES;
      case DOUBLE:
        return FieldType.DOUBLE;
      case FLOAT:
        return FieldType.FLOAT;
      case INTEGER:
        return FieldType.INTEGER;
      case LONG:
        return FieldType.LONG;
      case SHORT:
        return FieldType.SHORT;
      case STRING:
        return FieldType.STRING;
      case NULL:
      default:
        return FieldType.NULL;
    }
  }
}
