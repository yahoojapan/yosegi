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

public class TextStringFormatter implements ITextFormatter {
  @FunctionalInterface
  private interface WriteDispatcherFunc {
    public void accept(ByteArrayData buffer, Object obj) throws IOException;
  }

  protected static final SwitchDispatcherFactory.Func<Class, WriteDispatcherFunc> writeDispatcher;

  static {
    SwitchDispatcherFactory<Class, WriteDispatcherFunc> sw = new SwitchDispatcherFactory<>();
    sw.setDefault((buffer, obj) -> { });
    sw.set(byte[].class, (buf, obj) -> buf.append((byte[])obj));
    sw.set(String.class, (buf, obj) -> buf.append(((String)obj).getBytes("UTF-8")));
    sw.set(PrimitiveObject.class, (buf, obj) -> buf.append(((PrimitiveObject)obj).getBytes()));
    writeDispatcher = sw.create();
  }

  @Override
  public void write(final ByteArrayData buffer, final Object obj) throws IOException {
    writeDispatcher.get(obj.getClass()).accept(buffer, obj);
  }

  @Override
  public void writeParser(
      final ByteArrayData buffer,
      final PrimitiveObject obj,
      final IParser parser) throws IOException {
    buffer.append(obj.getBytes());
  }
}

