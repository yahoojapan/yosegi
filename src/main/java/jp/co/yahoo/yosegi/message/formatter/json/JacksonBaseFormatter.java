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

package jp.co.yahoo.yosegi.message.formatter.json;

import com.fasterxml.jackson.databind.JsonNode;

import jp.co.yahoo.yosegi.message.objects.ObjectToJsonNode;
import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JacksonBaseFormatter {
  @FunctionalInterface
  public interface WriteDispatcherFunc {
    public JsonNode apply(Object obj) throws IOException;
  }

  protected static final SwitchDispatcherFactory.Func<Class, WriteDispatcherFunc> writeDispatcher;

  static {
    SwitchDispatcherFactory<Class, WriteDispatcherFunc> sw = new SwitchDispatcherFactory<>();
    sw.set(List.class, child -> JacksonContainerToJsonObject.getFromList((List<Object>)child));
    sw.set(List.class, child -> JacksonContainerToJsonObject.getFromMap((Map<Object,Object>)child));
    sw.setDefault(child -> ObjectToJsonNode.get(child));
    writeDispatcher = sw.create();
  }
}

