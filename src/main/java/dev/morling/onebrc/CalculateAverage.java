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

import com.sun.source.tree.Tree;

import static java.util.stream.Collectors.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage {

    private static final String FILE = "./measurements.txt";
    private static final int THREADS = 8;
    private static final int BUFFER_SIZE = 1_000_000_000;

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


        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(FILE), 2_000_000_000);


        Map<String, Holder> collect = new BufferedReader(new InputStreamReader(bufferedInputStream)).lines().parallel().map(s -> {
            String[] split = s.split(";");
            double v = Double.parseDouble(split[1]);
            return new Holder(split[0], v, 1, v, v);
        }).collect(groupingByConcurrent(Holder::name, Collector.of(Holder::new, Holder::merge, Holder::merge)));


        System.out.println((System.currentTimeMillis() - testStart) + " ms");

        System.out.print("{");
        String resultString = new TreeMap<>(collect).values().stream().map(Holder::toString).collect(Collectors.joining(", "));
        System.out.print(resultString);
        System.out.print("}\n");

        System.out.println((System.currentTimeMillis() - testStart) + " ms");
    }
}
