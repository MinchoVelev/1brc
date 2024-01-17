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
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.*;
import java.util.stream.Collectors;

public class CalculateAverage3 {

    private static final String FILE = "./measurements.txt";
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static final int BUFFER_SIZE = 60_000_000;

    static class Holder {
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

        public Holder() {
            this("", 0.0, 0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        }

        String name() {
            return name;
        }

        Holder merge(Holder holder2) {
            this.name = holder2.name;
            this.count += holder2.count;
            if (holder2.max > this.max) {
                this.max = holder2.max;
            }
            if (holder2.min < this.min) {
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

    public static void main(String[] args) throws Exception {
        System.out.println("Available processors: " + THREADS);
        long testStart = System.currentTimeMillis();
        FileInputStream fileStream = new FileInputStream(FILE);
        long fileSize = fileStream.getChannel().size();
        InputStreamReader inputStreamReader = new InputStreamReader(new BufferedInputStream(fileStream));


        List<Map<String, Holder>> resultMaps = Collections.synchronizedList(new ArrayList<>(32));
        LinkedList<Thread> threads = new LinkedList<>();

        long read = 0;
        int index = 0;
        for (; index < fileSize / BUFFER_SIZE - 1; index++) {
            long positionStart = (long) index * BUFFER_SIZE;
            long sizeToRead = fileSize - ((long) index) * BUFFER_SIZE;
            sizeToRead = Math.min(BUFFER_SIZE, sizeToRead);

            char[] bytes = new char[(int) sizeToRead];
            int bytesRead = inputStreamReader.read(bytes);
            if (bytesRead < 1) {
                System.out.println("Failed to read bytes for chunk " + index);
                continue;
            }

            final int indexToProcess = index;
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    HashMap<String, Holder> r = processChunk(bytes, indexToProcess);
                    resultMaps.add(r);
                } catch (CharacterCodingException e) {
                    throw new RuntimeException(e);
                }

            }));

            if (threads.size() == THREADS) {
                for (var t : threads) {
                    t.join();
                }
                threads.clear();
                System.out.println("Processed chunks: " + (index + 1) + " of " + (fileSize / BUFFER_SIZE + 1));
            }
        }


        for (var t : threads) {
            t.join();
        }


//            if (threads.size() == THREADS) {
//                for (var t : threads) {
//                    t.join();
//                }
//                threads.clear();
//                System.out.println("Processed chunks: " + index + " of " + size);
//            }


        //process remainders
        Map<String, Holder> first = resultMaps.get(0);
        for (int i = 0; i < index - 1; i++) {
            String line = remainders[i] + (startingParts[i + 1] == null ? "" : startingParts[i + 1]);

            if (line.isBlank()) {
                continue;
            }

            String[] split = line.split(";");
            double v = Double.parseDouble(split[1]);
            Holder newHolder = new Holder(split[0], v, 1, v, v);
            Holder tmp = first.get(newHolder.name);
            if (tmp != null) {
                first.put(newHolder.name, tmp.merge(newHolder));
            } else {
                first.put(newHolder.name, newHolder);
            }
        }

        System.out.println((System.currentTimeMillis() - testStart) + " ms - finished the file processing");

        ///merge all maps

        for (int i = 1; i < resultMaps.size(); i++) {
            for (var h : resultMaps.get(i).values()) {
                Holder tmp = first.get(h.name);
                if (tmp != null) {
                    first.put(h.name, tmp.merge(h));
                } else {
                    first.put(h.name, h);
                }
            }
        }


        System.out.println((System.currentTimeMillis() - testStart) + " ms - merged the maps");


        System.out.print("{");
        String resultString = new TreeMap<>(first).values().stream().map(Holder::toString).collect(Collectors.joining(", "));
        System.out.print(resultString);
        System.out.print("}\n");

        System.out.println((System.currentTimeMillis() - testStart) + " ms finished");
    }

    static String[] remainders = new String[5000];
    static String[] startingParts = new String[5000];

    static HashMap<String, Holder> processChunk(char[] chunk, int index) throws CharacterCodingException {
        HashMap<String, Holder> resultsMap = new HashMap<>();
        StringBuilder builder = new StringBuilder(512);
        int charI = 0;
        if (index != 0) {
            for (; charI < chunk.length; charI++) {
                char c = chunk[charI];
                if (c == '\n') {
                    charI += 1;
                    break;
                }
                builder.append(c);
            }

            startingParts[index] = builder.toString();
            builder = new StringBuilder(512);
        }
        for (; charI < chunk.length; charI++) {
            char c1 = chunk[charI];
            if (c1 == '\n') {
                String line = builder.toString();

                String[] split = line.split(";");
                double v = Double.parseDouble(split[1]);

                Holder newHolder = new Holder(split[0], v, 1, v, v);
                Holder tmp = resultsMap.get(split[0]);
                if (tmp == null) {
                    resultsMap.put(newHolder.name, newHolder);
                } else {
                    resultsMap.put(newHolder.name, tmp.merge(newHolder));
                }

                builder = new StringBuilder(512);
                continue;
            }
            builder.append(c1);

        }

        remainders[index] = builder.toString();

        return resultsMap;
    }
}
