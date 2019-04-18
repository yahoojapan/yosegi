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

package jp.co.yahoo.yosegi.message.objects;

public final class JavaObjectToPrimitiveObject {

  private JavaObjectToPrimitiveObject() {}

  /**
   * Convert Java objects to PrimitiveObject.
   */
  public static PrimitiveObject get( final Object obj ) {
    if ( obj instanceof PrimitiveObject ) {
      return (PrimitiveObject)obj;
    } else if ( obj instanceof Boolean ) {
      return new BooleanObj( (Boolean)obj );
    } else if ( obj instanceof Byte ) {
      return new ByteObj( (Byte)obj );
    } else if ( obj instanceof byte[] ) {
      return new BytesObj( (byte[])obj );
    } else if ( obj instanceof Double ) {
      return new DoubleObj( (Double)obj );
    } else if ( obj instanceof Float ) {
      return new FloatObj( (Float)obj );
    } else if ( obj instanceof Integer ) {
      return new IntegerObj( (Integer)obj );
    } else if ( obj instanceof Long ) {
      return new LongObj( (Long)obj );
    } else if ( obj instanceof Short ) {
      return new ShortObj( (Short)obj );
    } else if ( obj instanceof String ) {
      return new StringObj( (String)obj );
    }
    return NullObj.getInstance();
  }

}
