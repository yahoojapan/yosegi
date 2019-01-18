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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;

public class PrimitiveCell implements ICell<PrimitiveObject,PrimitiveObject> {

  private final ColumnType columnType;
  private PrimitiveObject raw;

  public PrimitiveCell( final ColumnType columnType , final PrimitiveObject raw ) {
    this.raw = raw;
    this.columnType = columnType;
  }

  @Override
  public PrimitiveObject getRow() {
    return raw;
  }

  @Override
  public ColumnType getType() {
    return columnType;
  }

  @Override
  public void setRow( final PrimitiveObject raw ) {
    this.raw = raw;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    try {
      result.append( String.format( "(%s)" , getType() ) );
      result.append( raw.getString() );
    } catch ( IOException ex ) {
      result.append( "[ERROR]" );
    }

    return result.toString();
  }

}
