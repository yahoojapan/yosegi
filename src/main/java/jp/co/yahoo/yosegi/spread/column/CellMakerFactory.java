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

public final class CellMakerFactory {

  /**
   * Create cell maker.
   */
  public static ICellMaker getCellMaker( final ColumnType columnType ) throws IOException {
    switch ( columnType ) {
      case BOOLEAN:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new BooleanCell( obj );
          }
        };
      case BYTE:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new ByteCell( obj );
          }
        };
      case BYTES:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new BytesCell( obj );
          }
        };
      case DOUBLE:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new DoubleCell( obj );
          }
        };
      case FLOAT:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new FloatCell( obj );
          }
        };
      case INTEGER:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new IntegerCell( obj );
          }
        };
      case LONG:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new LongCell( obj );
          }
        };
      case SHORT:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new ShortCell( obj );
          }
        };
      case STRING:
        return new ICellMaker() {
          @Override
          public PrimitiveCell create( final PrimitiveObject obj ) {
            return new StringCell( obj );
          }
        };
      default:
        throw new IOException( "Unknown column type : " + columnType.toString() );
    }
  }

}
