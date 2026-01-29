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

package org.apache.drill.exec.store.swift;

import com.prowidesoftware.swift.model.BIC;
import com.prowidesoftware.swift.model.SwiftBlock1;
import com.prowidesoftware.swift.model.SwiftBlock2;
import com.prowidesoftware.swift.model.SwiftBlock2Input;
import com.prowidesoftware.swift.model.SwiftBlock2Output;
import com.prowidesoftware.swift.model.SwiftBlock3;
import com.prowidesoftware.swift.model.SwiftBlock4;
import com.prowidesoftware.swift.model.SwiftBlock5;
import com.prowidesoftware.swift.model.SwiftMessage;
import com.prowidesoftware.swift.model.Tag;
import com.prowidesoftware.swift.model.field.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;

public class SwiftUtils {

    private static final Logger logger = LoggerFactory.getLogger(SwiftUtils.class);

    public static HashMap<String, String> parseMessage(String message) {
        SwiftMessage swiftMessage;
        Locale locale = Locale.getDefault();
        try {
            swiftMessage = SwiftMessage.parse(message);
        } catch (IOException e) {
            return null;
        }

        String messageType = swiftMessage.getType();
        HashMap<String, String> results = new HashMap<>();
        results.put("sender", swiftMessage.getSender());
        results.put("receiver", swiftMessage.getReceiver());
        results.put("UUID", swiftMessage.getUUID());
        results.put("message_type", messageType);
        results.put("block_count", String.valueOf(swiftMessage.getBlockCount()));

        // Process the BIC
        BIC correspondentBIC = swiftMessage.getCorrespondentBIC();
        if (correspondentBIC != null) {
            processBIC("correspondent_bic", correspondentBIC, results);
        }

        SwiftBlock1 block1 = swiftMessage.getBlock1();
        SwiftBlock2 block2 = swiftMessage.getBlock2();
        SwiftBlock3 block3 = swiftMessage.getBlock3();
        SwiftBlock4 block4 = swiftMessage.getBlock4();
        SwiftBlock5 block5 = swiftMessage.getBlock5();

        // Parse block 1
        if (block1 != null) {
            results.put("block1_name", block1.getName());
            results.put("block1_application_id", block1.getApplicationId());
            results.put("block1_service_id", block1.getServiceId());
            results.put("block1_logical_terminal", block1.getLogicalTerminal());
            results.put("block1_session_number", block1.getSessionNumber());
            results.put("block1_sequence_number", block1.getSequenceNumber());
        }

        // Parse block 2 - can be Input or Output type
        if (block2 != null) {
            // Common fields from SwiftBlock2
            results.put("block2_name", block2.getName());
            results.put("block2_block_value", block2.getBlockValue());
            results.put("block2_message_type", block2.getMessageType());
            results.put("block2_message_priority", block2.getMessagePriority());
            results.put("block2_is_input", String.valueOf(block2.isInput()));
            results.put("block2_is_output", String.valueOf(block2.isOutput()));
            results.put("block2_direction", block2.isInput() ? "Input" : "Output");

            if (block2 instanceof SwiftBlock2Input) {
                SwiftBlock2Input input = (SwiftBlock2Input) block2;
                // Full block value
                results.put("block2_input_value", input.getValue());
                // Receiver/destination
                results.put("block2_receiver_address", input.getReceiverAddress());
                // Delivery options
                results.put("block2_delivery_monitoring", input.getDeliveryMonitoring());
                results.put("block2_obsolescence_period", input.getObsolescencePeriod());
                // Derived delivery monitoring description
                String dm = input.getDeliveryMonitoring();
                if (dm != null) {
                    String dmDesc = "";
                    switch (dm) {
                        case "1": dmDesc = "Non-Delivery Warning"; break;
                        case "2": dmDesc = "Delivery Notification"; break;
                        case "3": dmDesc = "Non-Delivery Warning and Delivery Notification"; break;
                        default: dmDesc = dm;
                    }
                    results.put("block2_delivery_monitoring_desc", dmDesc);
                }
                // Priority description
                String priority = input.getMessagePriority();
                if (priority != null) {
                    String priorityDesc = "";
                    switch (priority) {
                        case "S": priorityDesc = "System"; break;
                        case "U": priorityDesc = "Urgent"; break;
                        case "N": priorityDesc = "Normal"; break;
                        default: priorityDesc = priority;
                    }
                    results.put("block2_priority_desc", priorityDesc);
                }
            } else if (block2 instanceof SwiftBlock2Output) {
                SwiftBlock2Output output = (SwiftBlock2Output) block2;
                // Full block value
                results.put("block2_output_value", output.getValue());
                // Sender information
                results.put("block2_sender_input_time", output.getSenderInputTime());
                // MIR (Message Input Reference) - complete and components
                results.put("block2_mir", output.getMIR());
                results.put("block2_mir_date", output.getMIRDate());
                results.put("block2_mir_logical_terminal", output.getMIRLogicalTerminal());
                results.put("block2_mir_session_number", output.getMIRSessionNumber());
                results.put("block2_mir_sequence_number", output.getMIRSequenceNumber());
                // Receiver output information
                results.put("block2_receiver_output_date", output.getReceiverOutputDate());
                results.put("block2_receiver_output_time", output.getReceiverOutputTime());
                // Priority description
                String priority = output.getMessagePriority();
                if (priority != null) {
                    String priorityDesc = "";
                    switch (priority) {
                        case "S": priorityDesc = "System"; break;
                        case "U": priorityDesc = "Urgent"; break;
                        case "N": priorityDesc = "Normal"; break;
                        default: priorityDesc = priority;
                    }
                    results.put("block2_priority_desc", priorityDesc);
                }
            }
        }

        if (block3 != null) {
            for (Tag tag : block3.getTags()) {
                Field field = tag.asField();
                String label = Field.getLabel(field.getName(), messageType, null, java.util.Locale.getDefault());
                label = cleanUpFieldName(label);
                String value = field.getValueDisplay(locale);
                results.put(label, value);
            }
        }

        if (block4 != null) {
            for (Tag tag : block4.getTags()) {
                Field field = tag.asField();
                String label = Field.getLabel(field.getName(), messageType, null, java.util.Locale.getDefault());
                label = cleanUpFieldName(label);
                String value = field.getValueDisplay(locale);
                results.put(label, value);
            }
        }

        // Parse block 5 - Trailer block with system information
        if (block5 != null) {
            // Block-level properties
            results.put("block5_name", block5.getName());
            results.put("block5_tag_count", String.valueOf(block5.size()));
            results.put("block5_is_empty", String.valueOf(block5.isEmpty()));

            // Well-known trailer tags with direct accessors
            // MAC - Message Authentication Code
            Tag macTag = block5.getTagByName("MAC");
            if (macTag != null) {
                results.put("block5_mac", macTag.getValue());
            }

            // CHK - Checksum
            Tag chkTag = block5.getTagByName("CHK");
            if (chkTag != null) {
                results.put("block5_chk", chkTag.getValue());
            }

            // PDE - Possible Duplicate Emission
            Tag pdeTag = block5.getTagByName("PDE");
            if (pdeTag != null) {
                results.put("block5_pde", pdeTag.getValue());
                results.put("block5_pde_desc", "Possible Duplicate Emission");
            }

            // PDM - Possible Duplicate Message
            Tag pdmTag = block5.getTagByName("PDM");
            if (pdmTag != null) {
                results.put("block5_pdm", pdmTag.getValue());
                results.put("block5_pdm_desc", "Possible Duplicate Message");
            }

            // DLM - Delayed Message
            Tag dlmTag = block5.getTagByName("DLM");
            if (dlmTag != null) {
                results.put("block5_dlm", dlmTag.getValue());
                results.put("block5_dlm_desc", "Delayed Message");
            }

            // MRF - Message Reference
            Tag mrfTag = block5.getTagByName("MRF");
            if (mrfTag != null) {
                results.put("block5_mrf", mrfTag.getValue());
            }

            // TNG - Training
            Tag tngTag = block5.getTagByName("TNG");
            if (tngTag != null) {
                results.put("block5_tng", tngTag.getValue());
                results.put("block5_tng_desc", "Training Message");
            }

            // SYS - System Originated Message
            Tag sysTag = block5.getTagByName("SYS");
            if (sysTag != null) {
                results.put("block5_sys", sysTag.getValue());
                results.put("block5_sys_desc", "System Originated Message");
            }

            // Also iterate through all tags to capture any additional/custom tags
            for (Tag tag : block5.getTags()) {
                String tagName = tag.getName();
                String tagValue = tag.getValue();
                // Use block5_ prefix and tag name for any tags not already captured
                String fieldName = "block5_tag_" + tagName.toLowerCase();
                if (!results.containsKey(fieldName)) {
                    results.put(fieldName, tagValue);
                }
            }
        }

        return results;
    }

    /**
     * Cleans up the provided field name by removing all non-alphanumeric characters
     * and replacing them with underscores, then converting the result to lowercase.
     *
     * @param fieldName the original field name to be cleaned up
     * @return the cleaned-up field name with non-alphanumeric characters replaced by underscores and all characters in lowercase
     */
    public static String cleanUpFieldName(String fieldName) {
        return fieldName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
    }

    public static void processBIC(String fieldPrefix, BIC bic, HashMap<String, String> results) {
        String bicFieldName = fieldPrefix + "_";
        results.put(bicFieldName + "bic", bic.getBic8());
        results.put(bicFieldName + "bic11", bic.getBic11());
        results.put(bicFieldName + "institution", bic.getInstitution());
        results.put(bicFieldName +"country", bic.getCountry());
        results.put(bicFieldName +"location", bic.getLocation());
        results.put(bicFieldName + "banch", bic.getBranch());
    }
}
