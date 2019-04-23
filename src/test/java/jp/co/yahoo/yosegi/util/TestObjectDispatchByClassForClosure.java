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
package jp.co.yahoo.yosegi.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Objects;

public class TestObjectDispatchByClassForClosure {
  @FunctionalInterface
  public interface DispatchedFunc {
    public int get(int i);
  }

  private class DummyBase {}
  private class Dummy extends DummyBase {}
  private static ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = null;
  private int base = 0;

  private int dispatch(Object obj, int val) {
    if (Objects.isNull(dispatcher)) {
      ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
      sw.setDefault(i -> 0 + base);
      sw.set(Dummy.class, i -> i * 10 + base);
      sw.set(DummyBase.class, i -> i * 20 + base);
      dispatcher = sw.create();
    }
    ++ base;
    return dispatcher.get(obj).get(val);
  }

  @Test
  public void T_dispatchWithBaseIncrement() {
    DummyBase dummyBase = new DummyBase();
    assertEquals(dispatch(dummyBase, 10), 200 + 1); // base incremented
    assertEquals(dispatch(dummyBase, 20), 400 + 2); // base incremented

    Dummy dummy = new Dummy();
    assertEquals(dispatch(dummy, 10), 100 + 3); // base incremented
    assertEquals(dispatch(dummy, 20), 200 + 4); // base incremented
  }
}

