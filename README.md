go-hll
======

A go/golang implementation of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). ported from [java-hll](https://github.com/aggregateknowledge/java-hll) and it is storage-compatible with the java version.


Overview
--------

HyperLogLog (HLL) is a fixed-size, set-like structure used for distinct value counting with tunable precision. For example, in 1280 bytes HLL can estimate the count of tens of billions of distinct values with only a few percent error.

In addition to the algorithm proposed in the [original paper](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), this implementation is augmented to improve its accuracy and memory use without sacrificing much speed. See below for more details.

Algorithms
----------

A `hll` is a combination of different set/distinct-value-counting algorithms that can be thought of as a hierarchy, along with rules for moving up that hierarchy. In order to distinguish between said algorithms, we have given them names:

### `EMPTY` ###
A constant value that denotes the empty set.

### `EXPLICIT` ###
An explicit, unique, sorted list of integers in the set, which is maintained up to a fixed cardinality.

### `SPARSE` ###
A 'lazy', map-based implementation of HyperLogLog, a probabilistic set data structure. Only stores the indices and values of non-zero registers in a map, until the number of non-zero registers exceeds a fixed cardinality.

### `FULL` ###
A fully-materialized, list-based implementation of HyperLogLog. Explicitly stores the value of every register in a list ordered by register index.

Motivation
----------

Our motivation for augmenting the original HLL algorithm went something like this:

* Naively, a HLL takes `regwidth * 2^log2m` bits to store.
* In typical usage, `log2m = 11` and `regwidth = 5`, it requires 10,240 bits or 1,280 bytes.
* That's a lot of bytes!

The first addition to the original HLL algorithm came from realizing that 1,280 bytes is the size of 160 64-bit integers. So, if we wanted more accuracy at low cardinalities, we could just keep an explicit set of the inputs as a sorted list of 64-bit integers until we hit the 161st distinct value. This would give us the true representation of the distinct values in the stream while requiring the same amount of memory. (This is the `EXPLICIT` algorithm.)

The second came from the realization that we didn't need to store registers whose value was zero. We could simply represent the set of registers that had non-zero values as a map from index to values. This is map is stored as a list of index-value pairs that are bit-packed "short words" of length `log2m + regwidth`. (This is the `SPARSE` algorithm.)

Combining these two augmentations, we get a "promotion hierarchy" that allows the algorithm to be tuned for better accuracy, memory, or performance.

Initializing and storing a new `hll` object will simply allocate a small sentinel value symbolizing the empty set (`EMPTY`). When you add the first few values, a sorted list of unique integers is stored in an `EXPLICIT` set. When you wish to cease trading off accuracy for memory, the values in the sorted list are "promoted" to a `SPARSE` map-based HyperLogLog structure. Finally, when there are enough registers, the map-based HLL will be converted to a bit-packed `FULL` HLL structure.

Empirically, the insertion rate of `EMPTY`, `EXPLICIT`, and `SPARSE` representations is measured in 200k/s - 300k/s range, while the throughput of the `FULL` representation is in the millions of inserts per second on relatively new hardware ('10 Xeon).

Naturally, the cardinality estimates of the `EMPTY` and `EXPLICIT` representations is exact, while the `SPARSE` and `FULL` representations' accuracies are governed by the guarantees provided by the original HLL algorithm.

* * * * * * * * * * * * * * * * * * * * * * * * *


The Importance of Hashing
=========================

In brief, it is absolutely crucial to hash inputs to an HLL. A close approximation of uniform randomness in the inputs ensures that the error guarantees laid out in the original paper hold. We've empirically determined that [MurmurHash 3](http://guava-libraries.googlecode.com/git/guava/src/com/google/common/hash/Murmur3_128HashFunction.java), from Google's Guava, is an excellent and fast hash function to use in conjunction with `java-hll` module.

The seed to the hash call must remain constant for all inputs to a given HLL.  Similarly, if one plans to compute the union of two HLLs, the input values must have been hashed using the same seed.

For a good overview of the importance of hashing and hash functions when using probabilistic algorithms as well as an analysis of MurmurHash 3, refer to these blog posts:

* [K-Minimum Values: Sketching Error, Hash Functions, and You](http://blog.aggregateknowledge.com/2012/08/20/k-minimum-values-sketching-error-hash-functions-and-you/)
* [Choosing a Good Hash Function, Part 1](http://blog.aggregateknowledge.com/2011/12/05/choosing-a-good-hash-function-part-1/)
* [Choosing a Good Hash Function, Part 2](http://blog.aggregateknowledge.com/2011/12/29/choosing-a-good-hash-function-part-2/)
* [Choosing a Good Hash Function, Part 3](http://blog.aggregateknowledge.com/2012/02/02/choosing-a-good-hash-function-part-3/)


On Unions and Intersections
===========================

HLLs have the useful property that the union of any number of HLLs is equal to the HLL that would have been populated by playing back all inputs to those '_n_' HLLs into a single HLL. Colloquially, one can say that HLLs have "lossless" unions because the same cardinality error guarantees that apply to a single HLL apply to a union of HLLs. See the `union()` function.

Using the [inclusion-exclusion principle](http://en.wikipedia.org/wiki/Inclusion%E2%80%93exclusion_principle) and the `Union()` function, one can also estimate the intersection of sets represented by HLLs. Note, however, that error is proportional to the union of the two HLLs, while the result can be significantly smaller than the union, leading to disproportionately large error relative to the actual intersection cardinality. For instance, if one HLL has a cardinality of 1 billion, while the other has a cardinality of 10 million, with an overlap of 5 million, the intersection cardinality can easily be dwarfed by even a 1% error estimate in the larger HLLs cardinality.

For more information on HLL intersections, see [this blog post](http://blog.aggregateknowledge.com/2012/12/17/hll-intersections-2/).

Usage
=====

HLL is available in Maven Central. Include it in your project with:

import "github.com/l0vest0rm/hll"


Hashing and adding a value to a new HLL:

```go
package main

import(
    "github.com/l0vest0rm/hll"
    "fmt"
    "math/rand"
    "io/ioutil"
    "bytes"
    "encoding/binary"
    "os"
    "time"
)

func main(){
    count := 40000000
    var clientid uint64

    buf := bytes.NewBuffer([]byte{})
    for i := 0; i < count; i++ {
        clientid = uint64(rand.Int63())
        binary.Write(buf, binary.LittleEndian, clientid)
    }
    b := buf.Bytes()

    t1 := time.Now().UnixNano()
    h,err := hll.NewHll(14, 6)
    if err != nil {
        panic(fmt.Sprintf("hll.NewHll err:%s", err.Error()))
    }

    offset := 0
    for i := 0; i < count; i++ {
        clientid = binary.LittleEndian.Uint64(b[offset:])
        h.Add(clientid)
        offset += 8
    }

    num := h.Cardinality()
    t2 := time.Now().UnixNano()
    fmt.Printf("time:%d ns,accuracy:%f\n", t2-t1, float64(num)/float64(count))
    data := h.ToBytes()
    fmt.Printf("bytes:%d\n", len(data))

    filename := "/tmp/hyperloglog.dat"
    err = ioutil.WriteFile(filename, data, os.ModePerm)
    if err != nil {
        fmt.Printf("ioutil.WriteFile fail,err:%s\n", err.Error())
        return
    }

    data, err = ioutil.ReadFile(filename)
    if err != nil {
        fmt.Printf("ioutil.ReadFile,err:%s\n", err.Error())
        return
    }

    t3 := time.Now().UnixNano()
    h2, err := hll.NewHllFromBytes(data)
    if err != nil {
        fmt.Printf("hll.NewHllFromBytes,err:%s\n", err.Error())
        return
    }

    num = h2.Cardinality()
    t4 := time.Now().UnixNano()

    //merge
    h.Union(h2)

    fmt.Printf("time:%d ns,accuracy:%f,after union accuracy:%f\n", t4-t3, float64(num)/float64(count), float64(h.Cardinality())/float64(count))

}
```

Retrieving the cardinality of an HLL:

```go
    cardinality := hll.Cardinality();
```

Unioning two HLLs together (and retrieving the resulting cardinality):

```go

    hll1.Union(hll2)/*modifies hll1 to contain the union*/;
    cardinalityUnion := hll1.Cardinality()
```

Reading an HLL from a hex representation of [storage specification, v1.0.0](https://github.com/aggregateknowledge/hll-storage-spec/blob/v1.0.0/STORAGE.md) (for example, retrieved from a [PostgreSQL database](https://github.com/aggregateknowledge/postgresql-hll)):

```go
    hll := hll.NewHllFromBytes(bytes);
```

Writing an HLL to its hex representation of [storage specification, v1.0.0](https://github.com/aggregateknowledge/hll-storage-spec/blob/v1.0.0/STORAGE.md) (for example, to be inserted into a [PostgreSQL database](https://github.com/aggregateknowledge/postgresql-hll)):

```go
    bytes := hll.ToBytes();
```

