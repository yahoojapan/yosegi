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

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.ByteField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.UnionField;

import java.io.IOException;

public final class ColumnBinaryToSchema {

  private ColumnBinaryToSchema() {}

  /**
   * Determine the type of ColumnBinary and convert it to IField.
   */
  public static IField get( final ColumnBinary columnBinary ) throws IOException {
    switch ( columnBinary.columnType ) {
      case UNION:
        UnionField unionSchema = new UnionField( columnBinary.columnName );
        for ( ColumnBinary childBinary : columnBinary.columnBinaryList ) {
          unionSchema.set( get( childBinary ) );
        }
        return unionSchema;
      case ARRAY:
        return new ArrayContainerField(
            columnBinary.columnName , get( columnBinary.columnBinaryList.get(0) ) );
      case SPREAD:
        StructContainerField structSchema = new StructContainerField( columnBinary.columnName );
        for ( ColumnBinary childBinary : columnBinary.columnBinaryList ) {
          structSchema.set( get( childBinary ) );
        }
        return structSchema;
      case BOOLEAN:
        return new BooleanField( columnBinary.columnName );
      case BYTE:
        return new ByteField( columnBinary.columnName );
      case BYTES:
        return new BytesField( columnBinary.columnName );
      case DOUBLE:
        return new DoubleField( columnBinary.columnName );
      case FLOAT:
        return new FloatField( columnBinary.columnName );
      case INTEGER:
        return new IntegerField( columnBinary.columnName );
      case LONG:
        return new LongField( columnBinary.columnName );
      case SHORT:
        return new ShortField( columnBinary.columnName );
      case STRING:
        return new StringField( columnBinary.columnName );
      default:
        return new NullField( columnBinary.columnName );
    }
  }

}
