//it is ported from [java-hll](https://github.com/aggregateknowledge/java-hll) and it is storage-compatible with the java version,thanks to the original author.
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
    "bytes"
)

const (
    // minimum and maximum values for the log-base-2 of the number of registers in the HLL
    MINIMUM_LOG2M_PARAM = 4
    MAXIMUM_LOG2M_PARAM = 30
    // minimum and maximum values for the register width of the HLL
    MINIMUM_REGWIDTH_PARAM = 1
    MAXIMUM_REGWIDTH_PARAM = 8
    // minimum and maximum values for the 'expthresh' parameter of the
    // constructor that is meant to match the PostgreSQL implementation's
    // constructor and parameter names
    MINIMUM_EXPTHRESH_PARAM = -1
    MAXIMUM_EXPTHRESH_PARAM = 18
    MAXIMUM_EXPLICIT_THRESHOLD = 1 << (MAXIMUM_EXPTHRESH_PARAM - 1)/*per storage spec*/
)

const (
    UNDEFINED = 0
    EMPTY = 1
    EXPLICIT = 2
    SPARSE = 3
    FULL = 4
)

type Hll struct {
    // ************************************************************************
    // Storage
    // storage used when #type is EXPLICIT, null otherwise
    explicitStorage            *LongHashSet
    // storage used when #type is SPARSE, null otherwise
    sparseProbabilisticStorage *Int2ByteHashMap
    // storage used when #type is FULL, null otherwise
    probabilisticStorage       *BitVector

    // current type of this HLL instance, if this changes then so should the
    // storage used (see above)
    hllType                    int

    // ------------------------------------------------------------------------
    // Characteristic parameters
    // NOTE:  These members are named to match the PostgreSQL implementation's
    //        parameters.
    // log2(the number of probabilistic HLL registers)
    log2m                      uint
    // the size (width) each register in bits
    regwidth                   uint

    // ------------------------------------------------------------------------
    // Computed constants
    // ........................................................................
    // EXPLICIT-specific constants
    // flag indicating if the EXPLICIT representation should NOT be used
    explicitOff                bool
    // flag indicating that the promotion threshold from EXPLICIT should be
    // computed automatically
    // NOTE:  this only has meaning when 'explicitOff' is false
    explicitAuto               bool
    // threshold (in element count) at which a EXPLICIT HLL is converted to a
    // SPARSE or FULL HLL, always greater than or equal to zero and always a
    // power of two OR simply zero
    // NOTE:  this only has meaning when 'explicitOff' is false
    explicitThreshold          uint

    // ........................................................................
    // SPARSE-specific constants
    // the computed width of the short words
    shortWordLength            uint
    // flag indicating if the SPARSE representation should not be used
    sparseOff                  bool
    // threshold (in register count) at which a SPARSE HLL is converted to a
    // FULL HLL, always greater than zero
    sparseThreshold            uint

    // ........................................................................
    // Probabilistic algorithm constants
    // the number of registers, will always be a power of 2
    m                          uint
    // a mask of the log2m bits set to one and the rest to zero
    mBitsMask                  uint64
    // a mask as wide as a register (see #fromBytes())
    valueMask                  uint64
    // mask used to ensure that p(w) does not overflow register (see #Constructor() and #addRaw())
    pwMaxMask                  uint64
    // alpha * m^2 (the constant in the "'raw' HyperLogLog estimator")
    alphaMSquared              float64
    // the cutoff value of the estimator for using the "small" range cardinality
    // correction formula
    smallEstimatorCutoff       float64
    // the cutoff value of the estimator for using the "large" range cardinality
    // correction formula
    largeEstimatorCutoff       float64
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
func NewHll(log2m uint, regwidth uint) (*Hll, error) {
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
func NewHll2(log2m uint, regwidth uint, expthresh int, sparseon bool, hllType int) (*Hll, error) {
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
    this.mBitsMask = uint64(this.m - 1)
    this.valueMask = (1 << regwidth) - 1
    this.pwMaxMask = pwMaxMask(regwidth)
    this.alphaMSquared = alphaMSquared(float64(this.m))

    this.smallEstimatorCutoff = smallEstimatorCutoff(this.m)
    this.largeEstimatorCutoff = largeEstimatorCutoff(log2m, regwidth)

    if expthresh == -1 {
        this.explicitAuto = true
        this.explicitOff = false

        // NOTE:  This math matches the size calculation in the PostgreSQL impl.
        fullRepresentationSize := (this.regwidth * this.m + 7/*round up to next whole byte*/ ) / BYTES_PER_WORD
        numLongs := fullRepresentationSize / 8/*integer division to round down*/

        if (numLongs > MAXIMUM_EXPLICIT_THRESHOLD) {
            this.explicitThreshold = MAXIMUM_EXPLICIT_THRESHOLD;
        } else {
            this.explicitThreshold = uint(numLongs)
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
        return nil, fmt.Errorf("'expthresh' must be at least %d and at most %d (was %d)", MINIMUM_EXPTHRESH_PARAM, MAXIMUM_EXPTHRESH_PARAM, expthresh)
    }

    this.shortWordLength = (regwidth + log2m);
    this.sparseOff = !sparseon;
    if (this.sparseOff) {
        this.sparseThreshold = 0;
    } else {
        // TODO improve this cutoff to include the cost overhead of Java
        //      members/objects
        largestPow2LessThanCutoff := uint(math.Log2(float64(this.m * this.regwidth))) / this.shortWordLength
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
        this.explicitStorage, _ = NewLongHashSet()
        break;
    case SPARSE:
        this.sparseProbabilisticStorage, _ = NewInt2ByteHashMap()
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
func (this *Hll)Add(rawValue uint64) {
    switch(this.hllType) {
    case EMPTY:
        // NOTE:  EMPTY type is always promoted on #addRaw()
        if (this.explicitThreshold > 0) {
            this.initializeStorage(EXPLICIT);
            this.explicitStorage.Add(rawValue);
        } else if (!this.sparseOff) {
            this.initializeStorage(SPARSE);
            this.addRawSparseProbabilistic(rawValue);
        } else {
            this.initializeStorage(FULL);
            this.addRawProbabilistic(rawValue);
        }
        return;
    case EXPLICIT:
        this.explicitStorage.Add(rawValue)

        // promotion, if necessary
        if (this.explicitStorage.size > this.explicitThreshold) {
            if (!this.sparseOff) {
                this.initializeStorage(SPARSE);
                it := NewLongHashSetIterator(this.explicitStorage)
                for ; it.HasNext(); {
                    k := it.Next()
                    this.addRawSparseProbabilistic(k)
                }
            } else {
                this.initializeStorage(FULL);
                it := NewLongHashSetIterator(this.explicitStorage)
                for ; it.HasNext(); {
                    k := it.Next()
                    this.addRawProbabilistic(k)
                }
            }
            this.explicitStorage = nil
        }
        return
    case SPARSE: {
        this.addRawSparseProbabilistic(rawValue);

        // promotion, if necessary
        if (this.sparseProbabilisticStorage.size > this.sparseThreshold) {
            this.initializeStorage(FULL);
            it := NewInt2ByteHashMapIterator(this.sparseProbabilisticStorage)
            for ; it.HasNext(); {
                registerIndex := it.NextKey()
                registerValue := this.sparseProbabilisticStorage.get(registerIndex)
                this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
            }
            this.sparseProbabilisticStorage = nil
        }
        return;
    }
    case FULL:
        this.addRawProbabilistic(rawValue)
        return;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", this.hllType))
        return
    }
}

/**
     * Computes the cardinality of the HLL.
     *
     * @return the cardinality of HLL. This will never be negative.
     */
func (this *Hll)Cardinality() uint {
    switch(this.hllType) {
    case EMPTY:
        return 0/*by definition*/
    case EXPLICIT:
        return this.explicitStorage.Size()
    case SPARSE:
        return uint(math.Ceil(this.sparseProbabilisticAlgorithmCardinality()))
    case FULL:
        return uint(math.Ceil(this.fullProbabilisticAlgorithmCardinality()))
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", this.hllType))
        return 0
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
        p_w = byte((1 + leastSignificantBit(substreamValue | this.pwMaxMask)))
    }

    // Short-circuit if the register is being set to zero, since algorithmically
    // this corresponds to an "unset" register, and "unset" registers aren't
    // stored to save memory. (The very reason this sparse implementation
    // exists.) If a register is set to zero it will break the #algorithmCardinality
    // code.
    if (p_w == 0) {
        return;
    }

    // NOTE:  no +1 as in paper since 0-based indexing
    j := uint32(rawValue & this.mBitsMask)

    this.probabilisticStorage.setMaxRegister(uint64(j), uint64(p_w))
}

/**
     * Adds the raw value to the {@link #sparseProbabilisticStorage}.
     * {@link #type} must be {@link HLLType#SPARSE}.
     *
     * @param rawValue the raw value to add to the sparse storage.
     */
func (this *Hll)addRawSparseProbabilistic(rawValue uint64) {
    // p(w): position of the least significant set bit (one-indexed)
    // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
    //
    // By construction of pwMaxMask (see #Constructor()),
    //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
    // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
    // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
    substreamValue := (rawValue >> this.log2m);
    var p_w byte

    if (substreamValue == 0) {
        // The paper does not cover p(0x0), so the special value 0 is used.
        // 0 is the original initialization value of the registers, so by
        // doing this the multiset simply ignores it. This is acceptable
        // because the probability is 1/(2^(2^registerSizeInBits)).
        p_w = 0;
    } else {
        p_w = (byte)(1 + leastSignificantBit(substreamValue | this.pwMaxMask));
    }

    // Short-circuit if the register is being set to zero, since algorithmically
    // this corresponds to an "unset" register, and "unset" registers aren't
    // stored to save memory. (The very reason this sparse implementation
    // exists.) If a register is set to zero it will break the #algorithmCardinality
    // code.
    if (p_w == 0) {
        return
    }

    // NOTE:  no +1 as in paper since 0-based indexing
    j := uint32(rawValue & this.mBitsMask)

    currentValue := this.sparseProbabilisticStorage.get(j)
    if (p_w > currentValue) {
        this.sparseProbabilisticStorage.put(j, p_w)
    }
}

/**
     * Computes the exact cardinality value returned by the HLL algorithm when
     * represented as a {@link HLLType#FULL} HLL. Kept
     * separate from {@link #cardinality()} for testing purposes. {@link #type}
     * must be {@link HLLType#FULL}.
     *
     * @return the exact, unrounded cardinality given by the HLL algorithm
     */
func (this *Hll)fullProbabilisticAlgorithmCardinality() float64 {
    m := this.m/*for performance*/;

    // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
    // 'j'th register value
    sum, numberOfZeroes := this.probabilisticStorage.sum()

    // apply the estimate and correction to the indicator function
    estimator := this.alphaMSquared / sum
    if ((numberOfZeroes != 0) && (estimator < this.smallEstimatorCutoff)) {
        return smallEstimator(m, numberOfZeroes)
    } else if (estimator <= this.largeEstimatorCutoff) {
        return estimator;
    } else {
        return largeEstimator(this.log2m, this.regwidth, estimator);
    }
}

func (this *Hll) sparseProbabilisticAlgorithmCardinality() float64 {
    m := this.m/*for performance*/;

    // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
    // 'j'th register value
    sum := float64(0)
    numberOfZeroes := 0/*"V" in the paper*/;
    for j := uint(0); j < m; j++ {
        register := this.sparseProbabilisticStorage.get(uint32(j));

        sum += 1.0 / float64(uint64(1) << register)
        if register == 0 {
            numberOfZeroes++
        }
    }

    // apply the estimate and correction to the indicator function
    estimator := this.alphaMSquared / sum;
    if ((numberOfZeroes != 0) && (estimator < this.smallEstimatorCutoff)) {
        return smallEstimator(m, numberOfZeroes);
    } else if (estimator <= this.largeEstimatorCutoff) {
        return estimator;
    } else {
        return largeEstimator(this.log2m, this.regwidth, estimator);
    }
}

/**
     * Computes the union of HLLs and stores the result in this instance.
     *
     * @param other the other {@link HLL} instance to union into this one. This
     *        cannot be <code>null</code>.
     */
func (this *Hll) Union(other *Hll) {
    // TODO: verify HLLs are compatible
    if (this.hllType == other.hllType) {
        this.homogeneousUnion(other);
        return;
    } else {
        this.heterogenousUnion(other);
        return;
    }
}

/**
     * Computes the union of two HLLs of the same type, and stores the
     * result in this instance.
     *
     * @param other the other {@link HLL} instance to union into this one. This
     *        cannot be <code>null</code>.
     */
func (this *Hll) homogeneousUnion(other *Hll) {
    switch(this.hllType) {
    case EMPTY:
        // union of empty and empty is empty
        return;
    case EXPLICIT:
        it := NewLongHashSetIterator(other.explicitStorage)
        for ; it.HasNext(); {
            k := it.Next()
            this.Add(k)
        }
        // NOTE:  #addRaw() will handle promotion, if necessary
        return;
    case SPARSE:
        it := NewInt2ByteHashMapIterator(other.sparseProbabilisticStorage)
        for ; it.HasNext(); {
            registerIndex := it.NextKey()
            registerValue := other.sparseProbabilisticStorage.get(registerIndex)
            currentRegisterValue := this.sparseProbabilisticStorage.get(registerIndex)
            if (registerValue > currentRegisterValue) {
                this.sparseProbabilisticStorage.put(registerIndex, registerValue);
            }
        }

        // promotion, if necessary
        if (this.sparseProbabilisticStorage.size > this.sparseThreshold) {
            this.initializeStorage(FULL);
            it := NewInt2ByteHashMapIterator(this.sparseProbabilisticStorage)
            for ; it.HasNext(); {
                registerIndex := it.NextKey()
                registerValue := this.sparseProbabilisticStorage.get(registerIndex)
                this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
            }
            this.sparseProbabilisticStorage = nil
        }
        return;
    case FULL:
        for i := uint64(0); i<uint64(this.m); i++ {
            registerValue := other.probabilisticStorage.getRegister(i);
            this.probabilisticStorage.setMaxRegister(i, registerValue);
        }
        return;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", this.hllType))
        return
    }
}

// ------------------------------------------------------------------------
// Union helpers
/**
 * Computes the union of two HLLs, of different types, and stores the
 * result in this instance.
 *
 * @param other the other {@link HLL} instance to union into this one. This
 *        cannot be <code>null</code>.
 */
func (this *Hll) heterogenousUnion(other *Hll) {
    /*
     * The logic here is divided into two sections: unions with an EMPTY
     * HLL, and unions between EXPLICIT/SPARSE/FULL
     * HLL.
     *
     * Between those two sections, all possible heterogeneous unions are
     * covered. Should another type be added to HLLType whose unions
     * are not easily reduced (say, as EMPTY's are below) this may be more
     * easily implemented as Strategies. However, that is unnecessary as it
     * stands.
     */

    // ....................................................................
    // Union with an EMPTY
    if(this.hllType == EMPTY) {
            // NOTE:  The union of empty with non-empty HLL is just a
            //        clone of the non-empty.

        switch(other.hllType) {
        case EXPLICIT:
            // src:  EXPLICIT
            // dest: EMPTY

            if(other.explicitStorage.Size() <= this.explicitThreshold) {
                this.hllType = EXPLICIT
                this.explicitStorage = other.explicitStorage.Clone()
            } else {
                if(!this.sparseOff) {
                    this.initializeStorage(SPARSE)
                } else {
                    this.initializeStorage(FULL)
                }
                it := NewLongHashSetIterator(other.explicitStorage)
                for ; it.HasNext(); {
                    k := it.Next()
                    this.Add(k)
                }
            }
            return;
        case SPARSE:
            // src:  SPARSE
            // dest: EMPTY

            if(!this.sparseOff) {
                this.hllType = SPARSE
                this.sparseProbabilisticStorage = other.sparseProbabilisticStorage.Clone()
            } else {
                this.initializeStorage(FULL)
                it := NewInt2ByteHashMapIterator(other.sparseProbabilisticStorage)
                for ; it.HasNext(); {
                    registerIndex := it.NextKey()
                    registerValue := other.sparseProbabilisticStorage.get(registerIndex)
                    this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
                }
            }
            return;

        default/*case FULL*/:
            // src:  FULL
            // dest: EMPTY

            this.hllType = FULL
            this.probabilisticStorage = other.probabilisticStorage.Clone();
            return;
        }
    } else if other.hllType == EMPTY {
        // source is empty, so just return destination since it is unchanged
        return;
    } /* else -- both of the sets are not empty */

    // ....................................................................
    // NOTE: Since EMPTY is handled above, the HLLs are non-EMPTY below
    switch(this.hllType) {
    case EXPLICIT:
        // src:  FULL/SPARSE
        // dest: EXPLICIT
        // "Storing into destination" cannot be done (since destination
        // is by definition of smaller capacity than source), so a clone
        // of source is made and values from destination are inserted
        // into that.

        // Determine source and destination storage.
        // NOTE:  destination storage may change through promotion if
        //        source is SPARSE.
        if(other.hllType == SPARSE) {
            if(!this.sparseOff) {
                this.hllType = SPARSE
                this.sparseProbabilisticStorage = other.sparseProbabilisticStorage.Clone()
            } else {
                this.initializeStorage(FULL)
                it := NewInt2ByteHashMapIterator(other.sparseProbabilisticStorage)
                for ; it.HasNext(); {
                    registerIndex := it.NextKey()
                    registerValue := other.sparseProbabilisticStorage.get(registerIndex)
                    this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
                }
            }
        } else /*source is HLLType.FULL*/ {
            this.hllType = FULL
            this.probabilisticStorage = other.probabilisticStorage.Clone();
        }
        it := NewLongHashSetIterator(this.explicitStorage)
        for ; it.HasNext(); {
            k := it.Next()
            this.Add(k)
        }
        this.explicitStorage = nil;
        return;
    case SPARSE: {
        if(other.hllType == EXPLICIT) {
            // src:  EXPLICIT
            // dest: SPARSE
            // Add the raw values from the source to the destination.
            it := NewLongHashSetIterator(other.explicitStorage)
            for ; it.HasNext(); {
                k := it.Next()
                this.Add(k)
            }
            // NOTE:  addRaw will handle promotion cleanup
        } else /*source is HLLType.FULL*/ {
            // src:  FULL
            // dest: SPARSE
            // "Storing into destination" cannot be done (since destination
            // is by definition of smaller capacity than source), so a
            // clone of source is made and registers from the destination
            // are merged into the clone.

            this.hllType = FULL
            this.probabilisticStorage = other.probabilisticStorage.Clone();

            it := NewInt2ByteHashMapIterator(this.sparseProbabilisticStorage)
            for ; it.HasNext(); {
                registerIndex := it.NextKey()
                registerValue := this.sparseProbabilisticStorage.get(registerIndex)
                this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
            }
            this.sparseProbabilisticStorage = nil;
        }
        return;
    }
    default/*destination is HLLType.FULL*/:
        if(other.hllType == EXPLICIT) {
            // src:  EXPLICIT
            // dest: FULL
            // Add the raw values from the source to the destination.
            // Promotion is not possible, so don't bother checking.

            it := NewLongHashSetIterator(other.explicitStorage)
            for ; it.HasNext(); {
                k := it.Next()
                this.Add(k)
            }
        } else /*source is HLLType.SPARSE*/ {
            // src:  SPARSE
            // dest: FULL
            // Merge the registers from the source into the destination.
            // Promotion is not possible, so don't bother checking.

            it := NewInt2ByteHashMapIterator(other.sparseProbabilisticStorage)
            for ; it.HasNext(); {
                registerIndex := it.NextKey()
                registerValue := other.sparseProbabilisticStorage.get(registerIndex)
                this.probabilisticStorage.setMaxRegister(uint64(registerIndex), uint64(registerValue))
            }
        }
    }
}

/**
     * Serializes the HLL to an array of bytes in correspondence with the format
     * of the specified schema version.
     *
     * @param  schemaVersion the schema version dictating the serialization format
     * @return the array of bytes representing the HLL. This will never be
     *         <code>null</code> or empty.
     */
func (this *Hll) ToBytes() []byte {
    var bytes []byte

    switch(this.hllType) {
    case EMPTY:
        bytes = make([]byte, HEADER_BYTE_COUNT)
        break;
    case EXPLICIT:
        serializer := newBigEndianAscendingWordSerializer(BITS_PER_LONG, this.explicitStorage.Size())
        it := NewLongHashSetIterator(this.explicitStorage)
        for ; it.HasNext(); {
            k := it.Next()
            serializer.writeWord(k)
        }

        bytes = serializer.getBytes()
        break;
    case SPARSE:
        serializer := newBigEndianAscendingWordSerializer(this.shortWordLength, this.sparseProbabilisticStorage.Size())

        it := NewInt2ByteHashMapIterator(this.sparseProbabilisticStorage)
        for ; it.HasNext(); {
            registerIndex := it.NextKey()
            registerValue := this.sparseProbabilisticStorage.get(registerIndex)
            shortWord := ((uint64(registerIndex) << uint64(this.regwidth)) | uint64(registerValue))
            //binary.Write(buf, binary.BigEndian, shortWord)
            serializer.writeWord(shortWord)
        }

        bytes = serializer.getBytes()
        break;
    case FULL:
        serializer := newBigEndianAscendingWordSerializer(this.regwidth, this.m)

        it := NewBitVectorIterator(this.probabilisticStorage)
        for ; it.HasNext(); {
            serializer.writeWord(it.Next())
        }

        bytes = serializer.getBytes()
        break
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", this.hllType))
        return bytes
    }

    writeMetadata(bytes, this)

    return bytes
}

func (this *Hll) writeMetadata(buf *bytes.Buffer) {
    typeOrdinal := this.hllType

    var explicitCutoffValue int
    if(this.explicitOff) {
        explicitCutoffValue = EXPLICIT_OFF;
    } else if(this.explicitAuto) {
        explicitCutoffValue = EXPLICIT_AUTO;
    } else {
        explicitCutoffValue = int(math.Log2(float64(this.explicitThreshold)) + 1)/*per spec*/
    }

    buf.WriteByte(packVersionByte(SCHEMA_VERSION, typeOrdinal))
    buf.WriteByte(packParametersByte(this.regwidth, this.log2m))
    buf.WriteByte(packCutoffByte(explicitCutoffValue, !this.sparseOff))
}

/**
     * Deserializes the HLL (in {@link #toBytes(ISchemaVersion)} format) serialized
     * into <code>bytes</code>.<p/>
     *
     * @param  bytes the serialized bytes of new HLL
     * @return the deserialized HLL. This will never be <code>null</code>.
     *
     * @see #toBytes(ISchemaVersion)
     */
func NewHllFromBytes(bytes []byte) (*Hll, error) {
    if len(bytes) < HEADER_BYTE_COUNT {
        return nil, fmt.Errorf("too short bytes:%d", len(bytes))
    }

    versionByte := bytes[0]
    parametersByte := bytes[1]
    cutoffByte := bytes[2]

    //version := schemaVersion(versionByte)
    hllType := typeOrdinal(versionByte);
    explicitCutoffValue := explicitCutoff(cutoffByte);
    explicitOff := (explicitCutoffValue == EXPLICIT_OFF);
    explicitAuto := (explicitCutoffValue == EXPLICIT_AUTO);
    var log2ExplicitCutoff int
    if explicitOff || explicitAuto {
        log2ExplicitCutoff = -1
    }else {
        log2ExplicitCutoff = explicitCutoffValue - 1
    }

    regwidth := registerWidth(parametersByte)
    log2m := registerCountLog2(parametersByte)
    sparseon := sparseEnabled(cutoffByte)

    var expthresh int
    if explicitAuto {
        expthresh = -1;
    } else if explicitOff {
        expthresh = 0;
    } else {
        // NOTE: take into account that the postgres-compatible constructor
        //       subtracts one before taking a power of two.
        expthresh = log2ExplicitCutoff + 1
    }

    hll,err := NewHll2(log2m, regwidth, expthresh, sparseon, hllType)
    if err != nil {
        return nil, err
    }

    // Short-circuit on empty, which needs no other deserialization.
    if(hllType == EMPTY) {
        return hll, nil;
    }

    var wordLength uint
    switch(hllType) {
    case EXPLICIT:
        wordLength = BITS_PER_LONG
        break;
    case SPARSE:
        wordLength = hll.shortWordLength
        break;
    case FULL:
        wordLength = hll.regwidth
        break;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", hllType))
    }

    deserializer := newBigEndianAscendingWordDeserializer(wordLength, HEADER_BYTE_COUNT, bytes)

    switch(hllType) {
    case EXPLICIT:
        // NOTE:  This should not exceed expthresh and this will always
        //        be exactly the number of words that were encoded,
        //        because the word length is at least a byte wide.
        // SEE:   IWordDeserializer#totalWordCount()
        for i :=uint(0); i<deserializer.totalWordCount(); i++ {
            hll.explicitStorage.Add(deserializer.readWord());
        }
        break;
    case SPARSE:
        // NOTE:  If the shortWordLength were smaller than 8 bits
        //        (1 byte) there would be a possibility (because of
        //        padding arithmetic) of having one or more extra
        //        registers read. However, this is not relevant as the
        //        extra registers will be all zeroes, which are ignored
        //        in the sparse representation.
        for i :=uint(0); i<deserializer.totalWordCount(); i++ {
            shortWord := deserializer.readWord();
            registerValue := byte(shortWord & hll.valueMask)
            // Only set non-zero registers.
            if (registerValue != 0) {
                hll.sparseProbabilisticStorage.put(uint32(shortWord >> hll.regwidth), registerValue);
            }
        }
        break;
    case FULL:
        // NOTE:  Iteration is done using m (register count) and NOT
        //        deserializer#totalWordCount() because regwidth may be
        //        less than 8 and as such the padding on the 'last' byte
        //        may be larger than regwidth, causing an extra register
        //        to be read.
        // SEE: IWordDeserializer#totalWordCount()
        for i :=uint(0); i<deserializer.totalWordCount(); i++ {
            hll.probabilisticStorage.setRegister(uint64(i), deserializer.readWord());
        }
        break;
    default:
        panic(fmt.Sprintf("Unsupported HLL type %d", hllType))
    }

    return hll ,nil
}