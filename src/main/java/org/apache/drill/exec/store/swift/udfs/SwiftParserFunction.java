/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.swift.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

public class SwiftParserFunction {

    private SwiftParserFunction() {
    }

    @FunctionTemplate(
            names = {"swift_parse", "parse_swift"},
            scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class SwiftParse implements DrillSimpleFunc {
        @Param
        NullableVarCharHolder messageText;

        @Output
        BaseWriter.ComplexWriter outWriter;

        @Inject
        DrillBuf outBuffer;

        @Override
        public void setup() {
            // no op
        }

        @Override
        public void eval() {
            String raw_message = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(messageText);
            java.util.HashMap parsedSwiftData = org.apache.drill.exec.store.swift.SwiftUtils.parseMessage(raw_message);

            // If the message is empty or parsing failed, return an empty map
            if (messageText.isSet == 0 || parsedSwiftData == null) {
                org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();
                queryMapWriter.start();
                queryMapWriter.end();
                return;
            }


            try {
                org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();
                java.util.Iterator fieldIterator = parsedSwiftData.keySet().iterator();
                while (fieldIterator.hasNext()) {
                    String fieldName = (String) fieldIterator.next();
                    String field = (String) parsedSwiftData.get(fieldName);
                    org.apache.drill.exec.expr.holders.VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();

                    byte[] rowStringBytes = field.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    outBuffer = outBuffer.reallocIfNeeded(rowStringBytes.length);
                    outBuffer.setBytes(0, rowStringBytes);

                    rowHolder.start = 0;
                    rowHolder.end = rowStringBytes.length;
                    rowHolder.buffer = outBuffer;

                    queryMapWriter.varChar(fieldName).write(rowHolder);
                }
            } catch (Exception e) {
                // Log or handle error
            }
        }
    }
}
