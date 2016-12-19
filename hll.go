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

// Created by xuning on 2016/12/19

package hll

import (
    "fmt"
    "math"
)

const (
    // minimum and maximum values for the log-base-2 of the number of registers in the HLL
    MINIMUM_LOG2M_PARAM uint64 = 4
    MAXIMUM_LOG2M_PARAM uint64 = 30
    // minimum and maximum values for the register width of the HLL
    MINIMUM_REGWIDTH_PARAM uint64 = 1
    MAXIMUM_REGWIDTH_PARAM uint64 = 8
    // minimum and maximum values for the 'expthresh' parameter of the
    // constructor that is meant to match the PostgreSQL implementation's
    // constructor and parameter names
    MINIMUM_EXPTHRESH_PARAM = -1
    MAXIMUM_EXPTHRESH_PARAM = 18
    MAXIMUM_EXPLICIT_THRESHOLD = (1 << (MAXIMUM_EXPTHRESH_PARAM - 1)/*per storage spec*/);
)

const(
    EMPTY = 0
    EXPLICIT = 1
    SPARSE = 2
    FULL = 3
    UNDEFINED = 4
)

type Hll struct {
    // current type of this HLL instance, if this changes then so should the
    // storage used (see above)
    hllType int

    // ------------------------------------------------------------------------
    // Characteristic parameters
    // NOTE:  These members are named to match the PostgreSQL implementation's
    //        parameters.
    // log2(the number of probabilistic HLL registers)
    log2m uint64
    // the size (width) each register in bits
    regwidth uint64

    // ------------------------------------------------------------------------
    // Computed constants
    // ........................................................................
    // EXPLICIT-specific constants
    // flag indicating if the EXPLICIT representation should NOT be used
    explicitOff bool
    // flag indicating that the promotion threshold from EXPLICIT should be
    // computed automatically
    // NOTE:  this only has meaning when 'explicitOff' is false
    explicitAuto bool
    // threshold (in element count) at which a EXPLICIT HLL is converted to a
    // SPARSE or FULL HLL, always greater than or equal to zero and always a
    // power of two OR simply zero
    // NOTE:  this only has meaning when 'explicitOff' is false
    explicitThreshold int

    // ........................................................................
    // SPARSE-specific constants
    // the computed width of the short words
    shortWordLength uint64
    // flag indicating if the SPARSE representation should not be used
    sparseOff bool
    // threshold (in register count) at which a SPARSE HLL is converted to a
    // FULL HLL, always greater than zero
    sparseThreshold int

    // ........................................................................
    // Probabilistic algorithm constants
    // the number of registers, will always be a power of 2
    m uint64
    // a mask of the log2m bits set to one and the rest to zero
    mBitsMask uint64
    // a mask as wide as a register (see #fromBytes())
    valueMask uint64
    // mask used to ensure that p(w) does not overflow register (see #Constructor() and #addRaw())
    pwMaxMask uint64
    // alpha * m^2 (the constant in the "'raw' HyperLogLog estimator")
    alphaMSquared float64
    // the cutoff value of the estimator for using the "small" range cardinality
    // correction formula
    smallEstimatorCutoff float64
    // the cutoff value of the estimator for using the "large" range cardinality
    // correction formula
    largeEstimatorCutoff float64

    probabilisticStorage *BitVector
}

/**
     *  Construct an empty HLL with the given {@code log2m} and {@code regwidth}.<p/>
     *
     *  This is equivalent to calling <code>HLL(log2m, regwidth, -1, true, HLLType.EMPTY)</code>.
     *
     * @param log2m log-base-2 of the number of registers used in the HyperLogLog
     *        algorithm. Must be at least 4 and at most 30.
     * @param regwidth number of bits used per register in the HyperLogLog
     *        algorithm. Must be at least 1 and at most 8.
     *
     * @see #HLL(int, int, int, boolean, HLLType)
     */
func NewHll(log2m uint64, regwidth uint64, expthresh int, sparseon bool, hllType int) (*Hll, error) {
    Init()
    return NewHll2(log2m, regwidth, -1, true, EMPTY)
}

/**
     * NOTE: Arguments here are named and structured identically to those in the
     *       PostgreSQL implementation, which can be found
     *       <a href="https://github.com/aggregateknowledge/postgresql-hll/blob/master/README.markdown#explanation-of-parameters-and-tuning">here</a>.
     *
     * @param log2m log-base-2 of the number of registers used in the HyperLogLog
     *        algorithm. Must be at least 4 and at most 30.
     * @param regwidth number of bits used per register in the HyperLogLog
     *        algorithm. Must be at least 1 and at most 8.
     * @param expthresh tunes when the {@link HLLType#EXPLICIT} to
     *        {@link HLLType#SPARSE} promotion occurs,
     *        based on the set's cardinality. Must be at least -1 and at most 18.
     *        <table>
     *        <thead><tr><th><code>expthresh</code> value</th><th>Meaning</th></tr></thead>
     *        <tbody>
     *        <tr>
     *            <td>-1</td>
     *            <td>Promote at whatever cutoff makes sense for optimal memory usage. ('auto' mode)</td>
     *        </tr>
     *        <tr>
     *            <td>0</td>
     *            <td>Skip <code>EXPLICIT</code> representation in hierarchy.</td>
     *        </tr>
     *        <tr>
     *            <td>1-18</td>
     *            <td>Promote at 2<sup>expthresh - 1</sup> cardinality</td>
     *        </tr>
     *        </tbody>
     *        </table>
     * @param sparseon Flag indicating if the {@link HLLType#SPARSE}
     *        representation should be used.
     * @param type the type in the promotion hierarchy which this instance should
     *        start at. This cannot be <code>null</code>.
     */
func NewHll2(log2m uint64, regwidth uint64, expthresh int, sparseon bool, hllType int) (*Hll, error) {
    this := &Hll{}
    this.log2m = log2m
    if log2m < MINIMUM_LOG2M_PARAM || log2m > MAXIMUM_LOG2M_PARAM {
        return nil, fmt.Errorf("log2m must be at least %d and at most %d (was %d)", MINIMUM_LOG2M_PARAM, MAXIMUM_LOG2M_PARAM, log2m)
    }

    this.regwidth = regwidth;
    if regwidth < MINIMUM_REGWIDTH_PARAM || regwidth > MAXIMUM_REGWIDTH_PARAM {
        return nil, fmt.Errorf("regwidth must be at least %d and at most %d (was %d)", MINIMUM_REGWIDTH_PARAM, MAXIMUM_REGWIDTH_PARAM, regwidth)
    }

    this.m = (1 << log2m)
    this.mBitsMask = this.m - 1
    this.valueMask = (1 << regwidth) - 1
    this.pwMaxMask = pwMaxMask(regwidth)
    this.alphaMSquared = alphaMSquared(float64(this.m))

    this.smallEstimatorCutoff = smallEstimatorCutoff(this.m)
    this.largeEstimatorCutoff = largeEstimatorCutoff(log2m, regwidth)

    if expthresh == -1 {
        this.explicitAuto = true
        this.explicitOff = false

        // NOTE:  This math matches the size calculation in the PostgreSQL impl.
        fullRepresentationSize := (this.regwidth * this.m + 7/*round up to next whole byte*/)/8
        numLongs := fullRepresentationSize / 8/*integer division to round down*/

        if(numLongs > MAXIMUM_EXPLICIT_THRESHOLD) {
            this.explicitThreshold = MAXIMUM_EXPLICIT_THRESHOLD;
        } else {
            this.explicitThreshold = int(numLongs)
        }
    } else if expthresh == 0 {
        this.explicitAuto = false;
        this.explicitOff = true;
        this.explicitThreshold = 0;
    } else if expthresh > 0 && expthresh <= MAXIMUM_EXPTHRESH_PARAM {
        this.explicitAuto = false;
        this.explicitOff = false;
        this.explicitThreshold = (1 << (uint(expthresh) - 1))
    } else {
        return nil,fmt.Errorf("'expthresh' must be at least %d and at most %d (was %d)", MINIMUM_EXPTHRESH_PARAM, MAXIMUM_EXPTHRESH_PARAM, expthresh)
    }

    this.shortWordLength = (regwidth + log2m);
    this.sparseOff = !sparseon;
    if(this.sparseOff) {
        this.sparseThreshold = 0;
    } else {
        // TODO improve this cutoff to include the cost overhead of Java
        //      members/objects
        largestPow2LessThanCutoff := uint64(math.Log2(float64(this.m * this.regwidth))) / this.shortWordLength
        this.sparseThreshold = (1 << largestPow2LessThanCutoff);
    }

    this.initializeStorage(hllType)

    return this, nil
}

/**
     * Initializes storage for the specified {@link HLLType} and changes the
     * instance's {@link #type}.
     *
     * @param type the {@link HLLType} to initialize storage for. This cannot be
     *        <code>null</code> and must be an instantiable type. (For instance,
     *        it cannot be {@link HLLType#UNDEFINED}.)
     */
func (this *Hll)initializeStorage(hllType int) {
    this.hllType = hllType
    switch(hllType) {
    case EMPTY:
    // nothing to be done
    break;
    case EXPLICIT:
    //this.explicitStorage = new LongOpenHashSet();
    break;
    case SPARSE:
    //this.sparseProbabilisticStorage = new Int2ByteOpenHashMap();
    break;
    case FULL:
    this.probabilisticStorage = NewBitVector(this.regwidth, this.m)
    break;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", hllType))
    }
}

/**
     * Adds <code>rawValue</code> directly to the HLL.
     *
     * @param  rawValue the value to be added. It is very important that this
     *         value <em>already be hashed</em> with a strong (but not
     *         necessarily cryptographic) hash function. For instance, the
     *         Murmur3 implementation in
     *         <a href="http://guava-libraries.googlecode.com/git/guava/src/com/google/common/hash/Murmur3_128HashFunction.java">
     *         Google's Guava</a> library is an excellent hash function for this
     *         purpose and, for seeds greater than zero, matches the output
     *         of the hash provided in the PostgreSQL implementation.
     */
func (this *Hll)add(value uint64) {
    switch(this.hllType) {
    case FULL:
        this.addRawProbabilistic(value);
        return;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", this.hllType))
        return
    }
}

/**
     * Adds the raw value to the {@link #probabilisticStorage}.
     * {@link #type} must be {@link HLLType#FULL}.
     *
     * @param rawValue the raw value to add to the full probabilistic storage.
     */
func (this *Hll) addRawProbabilistic(rawValue uint64) {
    // p(w): position of the least significant set bit (one-indexed)
    // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
    //
    // By construction of pwMaxMask (see #Constructor()),
    //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
    // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
    // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
    substreamValue := (rawValue >> this.log2m)
    var p_w byte

    if substreamValue == 0 {
        // The paper does not cover p(0x0), so the special value 0 is used.
        // 0 is the original initialization value of the registers, so by
        // doing this the multiset simply ignores it. This is acceptable
        // because the probability is 1/(2^(2^registerSizeInBits)).
        p_w = 0
    } else {
        p_w = byte((1 + leastSignificantBit(substreamValue| this.pwMaxMask)))
    }

    // Short-circuit if the register is being set to zero, since algorithmically
    // this corresponds to an "unset" register, and "unset" registers aren't
    // stored to save memory. (The very reason this sparse implementation
    // exists.) If a register is set to zero it will break the #algorithmCardinality
    // code.
    if(p_w == 0) {
    return;
    }

    // NOTE:  no +1 as in paper since 0-based indexing
    j := uint32(rawValue & this.mBitsMask)

    this.probabilisticStorage.setMaxRegister(uint64(j), uint64(p_w))
}