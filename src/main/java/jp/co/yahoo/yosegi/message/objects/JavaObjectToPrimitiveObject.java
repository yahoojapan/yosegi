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

import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.util.function.Function;


public final class JavaObjectToPrimitiveObject {
  private interface DispatchedFunc extends Function<Object, PrimitiveObject> {}

  static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    /* CAUTION:
     * this structure is not the same function from original.
     * If there is a class derived from the following class,
     * it is necessary to branch by if then else if statement.
     */
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory<>();
    sw.setDefault(obj -> NullObj.getInstance());
    sw.set(PrimitiveObject.class, obj -> (PrimitiveObject)obj);
    sw.set(Boolean.class, obj -> new BooleanObj((Boolean)obj));
    sw.set(Byte.class,    obj -> new ByteObj(   (Byte)   obj));
    sw.set(byte[].class,  obj -> new BytesObj(  (byte[]) obj));
    sw.set(Double.class,  obj -> new DoubleObj( (Double) obj));
    sw.set(Float.class,   obj -> new FloatObj(  (Float)  obj));
    sw.set(Integer.class, obj -> new IntegerObj((Integer)obj));
    sw.set(Long.class,    obj -> new LongObj(   (Long)   obj));
    sw.set(Short.class,   obj -> new ShortObj(  (Short)  obj));
    sw.set(String.class,  obj -> new StringObj( (String) obj));
    dispatcher = sw.create();
  }

  private JavaObjectToPrimitiveObject() {}

  /**
   * Convert Java objects to PrimitiveObject.
   */
  public static PrimitiveObject get(final Object obj) {
    return dispatcher.get(obj.getClass()).apply(obj);
  }
}

