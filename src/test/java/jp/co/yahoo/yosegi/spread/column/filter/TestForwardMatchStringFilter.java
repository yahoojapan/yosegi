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
package jp.co.yahoo.yosegi.spread.column.filter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestForwardMatchStringFilter {

    @Test
    public void T_constractNotException() {
        new ForwardMatchStringFilter("string");
        new ForwardMatchStringFilter("");
        new ForwardMatchStringFilter(null);
    }

    @Test
    public void T_getSearchString() {
        ForwardMatchStringFilter target1 = new ForwardMatchStringFilter("string");
        ForwardMatchStringFilter target2 = new ForwardMatchStringFilter("");
        ForwardMatchStringFilter target3 = new ForwardMatchStringFilter(null);
        assertEquals(target1.getSearchString(), "string");
        assertEquals(target2.getSearchString(), "");
        assertEquals(target3.getSearchString(), null);
    }

    @Test
    public void T_getStringFilterType() {
        ForwardMatchStringFilter target = new ForwardMatchStringFilter("string");
        assertEquals(target.getStringFilterType(), StringFilterType.FORWARD);
    }

    @Test
    public void T_getFilterType() {
        ForwardMatchStringFilter target = new ForwardMatchStringFilter("string");
        assertEquals(target.getFilterType(), FilterType.STRING);
    }
}

