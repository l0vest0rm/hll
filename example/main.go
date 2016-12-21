/**
 * Copyright 2016 l0vest0rm.hll.example authors
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

// Created by xuning on 2016/12/21

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
    h,err := hll.NewHll(14, 5)
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