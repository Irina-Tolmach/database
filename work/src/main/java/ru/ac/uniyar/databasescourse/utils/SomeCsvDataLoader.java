package ru.ac.uniyar.databasescourse.utils;

import de.siegmar.fastcsv.reader.CsvReader;

import java.io.IOException;
import java.nio.file.Path;

public class SomeCsvDataLoader {
        public static void load(Path path) throws IOException {
            try (CsvReader csvReader = CsvReader.builder().build(path)) {
                // .skip(1) <=> skip the header
                csvReader.stream()/*.skip(1)*/.forEach(csvRow -> {System.out.println(csvRow.getField(0));});
            }
        }
}
