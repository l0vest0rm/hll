/**
 * Copyright 2016 l0vest0rm.hll authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

// Created by xuning on 2016/12/20

package hll

import(
    "math"
)

const (
    /**
         * The schema version number for this instance.
         */
    SCHEMA_VERSION = 1

    // number of header bytes for all HLL types
    HEADER_BYTE_COUNT = 3

    // sentinel values from the spec for explicit off and auto
    EXPLICIT_OFF = 0
    EXPLICIT_AUTO = 63
    /**
     * The number of bits (of the parameters byte) dedicated to encoding the
     * width of the registers.
     */
    REGISTER_WIDTH_BITS = 3

    /**
     * A mask to cap the maximum value of the register width.
     */
    REGISTER_WIDTH_MASK = (1 << REGISTER_WIDTH_BITS) - 1

    /**
     * The number of bits (of the parameters byte) dedicated to encoding
     * <code>log2(registerCount)</code>.
     */
    LOG2_REGISTER_COUNT_BITS = 5

    /**
     * A mask to cap the maximum value of <code>log2(registerCount)</code>.
     */
    LOG2_REGISTER_COUNT_MASK = (1 << LOG2_REGISTER_COUNT_BITS) - 1

    /**
     * The number of bits (of the cutoff byte) dedicated to encoding the
     * log-base-2 of the explicit cutoff or sentinel values for
     * 'explicit-disabled' or 'auto'.
     */
    EXPLICIT_CUTOFF_BITS = 6

    /**
     * A mask to cap the maximum value of the explicit cutoff choice.
     */
    EXPLICIT_CUTOFF_MASK = (1 << EXPLICIT_CUTOFF_BITS) - 1

    /**
     * Number of bits in a nibble.
     */
    NIBBLE_BITS = 4

    /**
     * A mask to cap the maximum value of a nibble.
     */
    NIBBLE_MASK = (1 << NIBBLE_BITS) - 1
)

/**
     * Generates a byte that encodes the schema version and the type ordinal
     * of the HLL.
     *
     * The top nibble is the schema version and the bottom nibble is the type
     * ordinal.
     *
     * @param schemaVersion the schema version to encode.
     * @param typeOrdinal the type ordinal of the HLL to encode.
     * @return the packed version byte
     */
func packVersionByte(schemaVersion int, typeOrdinal int) byte {
    return (byte)(((NIBBLE_MASK & schemaVersion) << NIBBLE_BITS) | (NIBBLE_MASK & typeOrdinal));
}

/**
     * Generates a byte that encodes the parameters of a
     * {@link HLLType#FULL} or {@link HLLType#SPARSE}
     * HLL.<p/>
     *
     * The top 3 bits are used to encode <code>registerWidth - 1</code>
     * (range of <code>registerWidth</code> is thus 1-9) and the bottom 5
     * bits are used to encode <code>registerCountLog2</code>
     * (range of <code>registerCountLog2</code> is thus 0-31).
     *
     * @param  registerWidth the register width (must be at least 1 and at
     *         most 9)
     * @param  registerCountLog2 the log-base-2 of the register count (must
     *         be at least 0 and at most 31)
     * @return the packed parameters byte
     */
func packParametersByte(registerWidth uint, registerCountLog2 uint) byte {
    widthBits := ((registerWidth - 1) & REGISTER_WIDTH_MASK);
    countBits := (registerCountLog2 & LOG2_REGISTER_COUNT_MASK);
    return (byte)((widthBits << LOG2_REGISTER_COUNT_BITS) | countBits);
}

/**
     * Generates a byte that encodes the log-base-2 of the explicit cutoff
     * or sentinel values for 'explicit-disabled' or 'auto', as well as the
     * boolean indicating whether to use {@link HLLType#SPARSE}
     * in the promotion hierarchy.
     *
     * The top bit is always padding, the second highest bit indicates the
     * 'sparse-enabled' boolean, and the lowest six bits encode the explicit
     * cutoff value.
     *
     * @param  explicitCutoff the explicit cutoff value to encode.
     *         <ul>
     *           <li>
     *             If 'explicit-disabled' is chosen, this value should be <code>0</code>.
     *           </li>
     *           <li>
     *             If 'auto' is chosen, this value should be <code>63</code>.
     *           </li>
     *           <li>
     *             If a cutoff of 2<sup>n</sup> is desired, for <code>0 <= n < 31</code>,
     *             this value should be <code>n + 1</code>.
     *           </li>
     *         </ul>
     * @param  sparseEnabled whether {@link HLLType#SPARSE}
     *         should be used in the promotion hierarchy to improve HLL
     *         storage.
     *
     * @return the packed cutoff byte
     */
func packCutoffByte(explicitCutoff int, sparseEnabled bool) byte {
    var sparseBit uint
    if sparseEnabled{
        sparseBit = 1 << EXPLICIT_CUTOFF_BITS
    }else{
        sparseBit = 0
    }

    return (byte)(sparseBit | (EXPLICIT_CUTOFF_MASK & uint(explicitCutoff)));
}

/**
     * Extracts the 'sparse-enabled' boolean from the cutoff byte of a serialized
     * HLL.
     *
     * @param  cutoffByte the cutoff byte of the serialized HLL
     * @return the 'sparse-enabled' boolean
     */
func sparseEnabled(cutoffByte byte) bool {
    return ((cutoffByte >> EXPLICIT_CUTOFF_BITS) & 1) == 1;
}

/**
 * Extracts the explicit cutoff value from the cutoff byte of a serialized
 * HLL.
 *
 * @param  cutoffByte the cutoff byte of the serialized HLL
 * @return the explicit cutoff value
 */
func explicitCutoff(cutoffByte byte) int {
    return int(cutoffByte & EXPLICIT_CUTOFF_MASK)
}

/**
 * Extracts the schema version from the version byte of a serialized
 * HLL.
 *
 * @param  versionByte the version byte of the serialized HLL
 * @return the schema version of the serialized HLL
 */
func schemaVersion(versionByte byte) int {
    return int(NIBBLE_MASK & (versionByte >> NIBBLE_BITS))
}

/**
 * Extracts the type ordinal from the version byte of a serialized HLL.
 *
 * @param  versionByte the version byte of the serialized HLL
 * @return the type ordinal of the serialized HLL
 */
func typeOrdinal(versionByte byte) int {
    return int(versionByte & NIBBLE_MASK)
}

/**
 * Extracts the register width from the parameters byte of a serialized
 * {@link HLLType#FULL} HLL.
 *
 * @param  parametersByte the parameters byte of the serialized HLL
 * @return the register width of the serialized HLL
 *
 * @see #packParametersByte(int, int)
 */
func registerWidth(parametersByte byte) uint {
    return uint(((parametersByte >> LOG2_REGISTER_COUNT_BITS) & REGISTER_WIDTH_MASK) + 1)
}

/**
 * Extracts the log2(registerCount) from the parameters byte of a
 * serialized {@link HLLType#FULL} HLL.
 *
 * @param  parametersByte the parameters byte of the serialized HLL
 * @return log2(registerCount) of the serialized HLL
 *
 * @see #packParametersByte(int, int)
 */
func registerCountLog2(parametersByte byte) uint {
    return uint(parametersByte & LOG2_REGISTER_COUNT_MASK)
}

func writeMetadata(bytes []byte,hll *Hll) {
    typeOrdinal := hll.hllType

    var explicitCutoffValue int
    if(hll.explicitOff) {
        explicitCutoffValue = EXPLICIT_OFF;
    } else if(hll.explicitAuto) {
        explicitCutoffValue = EXPLICIT_AUTO;
    } else {
        explicitCutoffValue = int(math.Log2(float64(hll.explicitThreshold)) + 1)/*per spec*/
    }

    bytes[0] = packVersionByte(SCHEMA_VERSION, typeOrdinal)
    bytes[1] = packParametersByte(hll.regwidth, hll.log2m)
    bytes[2] =packCutoffByte(explicitCutoffValue, !hll.sparseOff)
}