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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;
import java.util.Objects;

public class TextIntegerFormatter implements ITextFormatter {
  @FunctionalInterface
  private interface DispatchedFunc {
    public void accept(ByteArrayData buffer, Object obj) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory();
    sw.set(Short.class,   (buffer, obj) -> buffer.append(convert(((Short)  obj).intValue())));
    sw.set(Integer.class, (buffer, obj) -> buffer.append(convert(((Integer)obj).intValue())));
    sw.set(Long.class,    (buffer, obj) -> buffer.append(convert(((Long)   obj).intValue())));
    sw.set(Float.class,   (buffer, obj) -> buffer.append(convert(((Float)  obj).intValue())));
    sw.set(Double.class,  (buffer, obj) -> buffer.append(convert(((Double) obj).intValue())));
    sw.set(PrimitiveObject.class,
        (buffer, obj) -> buffer.append(convert(((PrimitiveObject)obj).getInt())));
    dispatcher = sw.create();
  }

  private static byte[] convert(final int target) throws IOException {
    return Integer.valueOf(target).toString().getBytes("UTF-8");
  }

  @Override
  public void write(final ByteArrayData buffer, final Object obj) throws IOException {
    DispatchedFunc ret = dispatcher.get(obj.getClass());
    if (Objects.nonNull(ret)) {
      ret.accept(buffer, obj);
    }
  }

  @Override
  public void writeParser(
      final ByteArrayData buffer ,
      final PrimitiveObject obj ,
      final IParser parser ) throws IOException {
    buffer.append( convert( ( (PrimitiveObject)obj ).getInt() ) );
  }
}

