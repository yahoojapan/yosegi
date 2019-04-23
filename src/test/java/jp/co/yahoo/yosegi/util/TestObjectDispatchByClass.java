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

public class TestObjectDispatchByClass {
  @FunctionalInterface
  public interface DispatchedFunc {
    public int get(int i);
  }

  private class DummyBase {}
  private class Dummy extends DummyBase {}

  @Test
  public void T_defaultReturnNull_1() {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = sw.create();

    int val = 1; // not set int class
    DispatchedFunc func = dispatcher.get(val);
    assertEquals(func, null); // default is not set, then null is returned
  }

  @Test
  public void T_defaultReturnNull_2() {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.set(Dummy.class, i -> i * 10);
    sw.set(DummyBase.class, i -> i * 20);
    ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = sw.create();

    int val = 1; // not set int class
    DispatchedFunc func = dispatcher.get(val);
    assertEquals(func, null); // defaut is not set, then null is returned
  }

  @Test
  public void T_defaultReturnSetValue() {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.setDefault(i -> 0); // set default
    sw.set(Dummy.class, i -> i * 10);
    sw.set(DummyBase.class, i -> i * 20);
    ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = sw.create();

    int val = 1; // not set int class
    DispatchedFunc func = dispatcher.get(val);
    assertEquals(func.get(10), 0); // then defauct setting is selected
  }

  @Test
  public void T_caseOfDispatching() {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.set(Dummy.class, i -> i * 10); // dispatching extended class first
    sw.set(DummyBase.class, i -> i * 20);
    ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = sw.create();

    // path the extended class
    DummyBase dummyBase = new DummyBase();
    DispatchedFunc funcBase = dispatcher.get(dummyBase);
    assertEquals(funcBase.get(10), 200);
    assertEquals(funcBase.get(20), 400);

    // catching extended class before parent class
    Dummy dummy = new Dummy();
    DispatchedFunc func = dispatcher.get(dummy);
    assertEquals(func.get(10), 100);
    assertEquals(func.get(20), 200);
  }

  @Test
  public void T_caseOfDispatchingBaseClassFirst() {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.set(DummyBase.class, i -> i * 20); // set parent class first
    sw.set(Dummy.class, i -> i * 10);
    ObjectDispatchByClass.Func<DispatchedFunc> dispatcher = sw.create();

    // catching base class
    DummyBase dummyBase = new DummyBase();
    DispatchedFunc funcBase = dispatcher.get(dummyBase);
    assertEquals(funcBase.get(10), 200);
    assertEquals(funcBase.get(20), 400);

    // select dummyBase function, because it sets before exetended one
    Dummy dummy = new Dummy();
    DispatchedFunc func = dispatcher.get(dummy);
    assertEquals(func.get(10), 200);
    assertEquals(func.get(20), 400);
  }
}

