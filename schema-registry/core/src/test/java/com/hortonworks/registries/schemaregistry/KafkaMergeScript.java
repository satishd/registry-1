/*
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaMergeScript {

    public static void main(String[] args) throws Exception {
        Path given = Paths.get(args[0]);
        Collection<File> files = FileUtils.listFiles(given.toFile(), new IOFileFilter() {
            @Override
            public boolean accept(File file) {
                return !file.getName().startsWith(".");
            }

            @Override
            public boolean accept(File file, String s) {
                return false;
            }
        }, new IOFileFilter() {
            @Override
            public boolean accept(File file) {
                return !file.getName().startsWith(".");
            }

            @Override
            public boolean accept(File file, String s) {
                return false;
            }
        });
        Map<File, List<Pair<Integer, Integer>>> toBeRemovedFileLines = new HashMap<>();
        for (File file : files) {
            System.out.println("file = " + file);
            List<String> lines = null;
            try {
                lines = Files.readAllLines(file.toPath());
            } catch (IOException e) {
                System.err.println("Error: " + e);
                continue;
            }
            List<Pair<Integer, Integer>> pairs = new ArrayList<>();
            Integer startNo = null, endNo = null;
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.startsWith("<<<<<<< HEAD")) {
                    startNo = i;
                } else if (line.startsWith("=======")) {
                    endNo = i;
                    if (startNo != null) {
                        pairs.add(Pair.of(startNo, endNo));
                    }
                } else if (line.startsWith(">>>>>>> 1.1.0")) {
                    pairs.add(Pair.of(i, i));
                }
            }

            if (!pairs.isEmpty()) {
                toBeRemovedFileLines.put(file, pairs);
            }
        }

        System.out.println("toBeRemovedFileLines = " + toBeRemovedFileLines);

        // remove lines from those files
        for (Map.Entry<File, List<Pair<Integer, Integer>>> entry : toBeRemovedFileLines.entrySet()) {
            final Map<Integer, Integer> linesMap = new HashMap<>();
            for (Pair<Integer, Integer> x : entry.getValue()) {
                System.out.println("################## x: " + x);
                linesMap.put(x.getKey(), x.getValue());
            }

            File tmpFile = Files.createTempFile("", "").toFile();
            tmpFile.deleteOnExit();

            File srcFile = entry.getKey();
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tmpFile));
                 BufferedReader bufferedReader = new BufferedReader(new FileReader(srcFile))) {
                String line;
                boolean stopWriting = false;
                Integer end = null;
                int i = 0;
                while ((line = bufferedReader.readLine()) != null) {

                    if (linesMap.containsKey(i)) {
                        stopWriting = true;
                        end = linesMap.get(i);
                    }

                    if (!stopWriting) {
                        bufferedWriter.write(line);
                        bufferedWriter.newLine();
                    }

                    if (end != null && end == i) {
                        stopWriting = false;
                    }

                    i++;
                }
            }

            FileUtils.copyFile(tmpFile, srcFile);
        }
    }
}
