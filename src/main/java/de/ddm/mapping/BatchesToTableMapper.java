package de.ddm.mapping;

import de.ddm.structures.Table;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author timo.buechert
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BatchesToTableMapper {

    /**
     * Converts batches to tables.
     * Improvement: Create an actor for the conversion and do it in parallel per table.
     *
     * @param headerLines header lines of the tables
     * @param batches     batches of the tables
     * @return tables
     */
    public static List<Table> convertToTables(final String[][] headerLines, final Map<Integer, List<List<String[]>>> batches) {
        final List<Table> tables = new ArrayList<>();
        for (int i = 0; i < headerLines.length; i++) {
            final Table table = new Table();
            table.setId(i);
            table.setHeader(Arrays.stream(headerLines[i]).collect(Collectors.toList()));
            table.setData(convertData(headerLines[i], batches.get(i)));

            tables.add(table);
            headerLines[i] = null;
            batches.remove(i);
        }
        return tables;
    }

    private static List<List<String>> convertData(final String[] headerLine, final List<List<String[]>> batches) {
        final List<List<String>> data = new ArrayList<>();

        for (int i = 0; i < headerLine.length; i++) {
            final List<String> column = new ArrayList<>();
            for (final List<String[]> batch : batches) {
                for (final String[] row : batch) {
                    column.add(row[i]);
                }
            }
            data.add(column);
        }

        return data;
    }
}
