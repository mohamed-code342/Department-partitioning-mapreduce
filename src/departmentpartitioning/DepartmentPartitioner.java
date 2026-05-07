package departmentpartitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DepartmentPartitioner
 *
 * Routes each department key to a dedicated reducer.
 *
 * Fixed routing table (upper-case comparison — case-insensitive):
 *   IT      → reducer 0
 *   HR      → reducer 1
 *   SALES   → reducer 2
 *   FINANCE → reducer 3
 *
 * Unknown departments fall back to hash-based partitioning.
 * All results are clamped to [0, numReduceTasks-1] to prevent
 * ArrayIndexOutOfBoundsException when numReduceTasks < 4.
 */
public class DepartmentPartitioner extends Partitioner<Text, Text> {

    private static final Map<String, Integer> ROUTING_TABLE;

    static {
        Map<String, Integer> m = new HashMap<>();
        m.put("IT",      0);
        m.put("HR",      1);
        m.put("SALES",   2);
        m.put("FINANCE", 3);
        ROUTING_TABLE = Collections.unmodifiableMap(m);
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        // Guard against null or empty key
        if (key == null || key.toString().trim().isEmpty()) {
            return 0 % numReduceTasks;
        }

        // Normalise: trim + upper-case for case-insensitive lookup
        String dept = key.toString().trim().toUpperCase();

        if (ROUTING_TABLE.containsKey(dept)) {
            // Clamp: protects against running fewer reducers than table assumes
            return ROUTING_TABLE.get(dept) % numReduceTasks;
        }

        // Fallback: hash-based distribution for unknown departments
        int hash = dept.hashCode();
        if (hash == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(hash) % numReduceTasks;
    }
}