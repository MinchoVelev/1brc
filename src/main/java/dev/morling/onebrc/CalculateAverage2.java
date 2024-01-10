/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage2 {

    private static final String FILE = "./measurements.txt";
    private static final int THREADS = 8;
    private static final int BUFFER_SIZE = 2_000_000_000;

    private static final int PAGE_SIZE = BUFFER_SIZE/THREADS;

    static class Holder{
        String name;
        Double value;
        int count;
        Double max;
        Double min;

        public Holder(String name, Double value, int count, Double max, Double min) {
            this.name = name;
            this.value = value;
            this.count = count;
            this.max = max;
            this.min = min;
        }

        public Holder(){
            this("", 0.0, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        }
        String name(){
            return name;
        }

        Holder merge(Holder holder2){
            this.name = holder2.name;
            this.count += holder2.count;
            if(holder2.max > this.max){
                this.max = holder2.max;
            }
            if(holder2.min < this.min){
                this.min = holder2.min;
            }
            this.value += holder2.value;
            return this;
        }

        @Override
        public String toString() {
           return name + "=" + min + "/" + String.format("%.1f", value / count) + "/" + max;
        }
    }

    public static void main(String[] args) throws IOException {
        long testStart = System.currentTimeMillis();


        System.out.println((System.currentTimeMillis() - testStart) + " ms");

        HashMap<String , Holder> collect = new HashMap<>();
        System.out.print("{");
        String resultString = new TreeMap<>(collect).values().stream().map(Holder::toString).collect(Collectors.joining(", "));
        System.out.print(resultString);
        System.out.print("}\n");

        System.out.println((System.currentTimeMillis() - testStart) + " ms");
    }
}
