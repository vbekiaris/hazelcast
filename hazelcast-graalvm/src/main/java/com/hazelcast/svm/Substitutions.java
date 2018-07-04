/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.svm;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.TargetClass;

import static com.oracle.svm.core.annotate.RecomputeFieldValue.Kind.ArrayBaseOffset;

@TargetClass(UnsafeUtil.class)
final class Target_com_hazelcast_internal_memory_impl_UnsafeUtil {
    @Alias
    @RecomputeFieldValue(kind = ArrayBaseOffset,
                        name = "ARRAY_BASE_OFFSET",
                        declClass = byte[].class)
    public static long ARRAY_BASE_OFFSET;
}

public class Substitutions {

}
