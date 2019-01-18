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

import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.ByteField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;

public final class PrimitiveSchemaFactory {
 
  /**
   * Create IField from column type.
   */
  public static IField getSchema( final ColumnType type , final String schemaName ) {
    switch ( type ) {
      case BOOLEAN:
        return new BooleanField( schemaName );
      case BYTE:
        return new ByteField( schemaName );
      case BYTES:
        return new BytesField( schemaName );
      case DOUBLE:
        return new DoubleField( schemaName );
      case FLOAT:
        return new FloatField( schemaName );
      case INTEGER:
        return new IntegerField( schemaName );
      case LONG:
        return new LongField( schemaName );
      case SHORT:
        return new ShortField( schemaName );
      case STRING:
        return new StringField( schemaName );

      default:
        return null;
    }

  }

}
