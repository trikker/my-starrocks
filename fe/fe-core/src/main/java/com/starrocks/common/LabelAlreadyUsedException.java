// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.google.common.base.Preconditions;
import com.google.re2j.Pattern;
import com.starrocks.transaction.TransactionStatus;
import org.apache.commons.lang3.StringUtils;

public class LabelAlreadyUsedException extends DdlException {

    private static final long serialVersionUID = -6798925248765094813L;
    private static final Pattern ERROR_PATTERN = Pattern.compile(".*Label \\[.*\\] has already been used.*");

    // status of existing load job
    // RUNNING or FINISHED
    private String jobStatus;

    public LabelAlreadyUsedException(String label) {
        super("Label [" + label + "] has already been used.");
    }

    public LabelAlreadyUsedException(String label, TransactionStatus txnStatus) {
        super("Label [" + label + "] has already been used.");
        switch (txnStatus) {
            case UNKNOWN:
            case PREPARE:
            case PREPARED:
                jobStatus = "RUNNING";
                break;
            case COMMITTED:
            case VISIBLE:
                jobStatus = "FINISHED";
                break;
            default:
                Preconditions.checkState(false, txnStatus);
                break;
        }
    }

    public LabelAlreadyUsedException(String label, String subLabel) {
        super("Sub label [" + subLabel + "] has already been used.");
    }

    public static boolean isLabelAlreadyUsed(String errorMessage) {
        return StringUtils.isNotEmpty(errorMessage) && ERROR_PATTERN.matches(errorMessage);
    }

    public String getJobStatus() {
        return jobStatus;
    }
}
