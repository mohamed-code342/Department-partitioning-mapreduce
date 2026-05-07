package departmentpartitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SalaryCombiner
 *
 * Local pre-aggregator. Runs on the mapper node before shuffle.
 * Hadoop may invoke this zero, one, or multiple times per spill file.
 *
 * Input  : (department, ["5000", "6000", "15800,3"])
 *            └ raw salaries from mapper OR partial aggregates from a prior
 *              combiner pass on an earlier spill
 *
 * Output : (department, "partialTotal,count")
 *   e.g.   ("IT",       "26800,5")
 *
 * CONTRACT: The comma-delimited format "total,count" is the ONLY format
 * this class emits. SalaryReducer must accept both this format AND raw
 * salary strings (for the case where the combiner did not run).
 *
 * IMPORTANT: salary values never contain a comma (they are plain integers
 * or decimals). A comma in a token therefore unambiguously identifies a
 * partial aggregate. This assumption is validated in EmployeeMapper which
 * rejects any salary that does not parse as a plain double.
 */
public class SalaryCombiner extends Reducer<Text, Text, Text, Text> {

    private static final String DELIM        = ",";
    private static final String COUNTER_GROUP = "SalaryCombiner.DataQuality";

    private final Text          outKey   = new Text();
    private final Text          outValue = new Text();
    private final StringBuilder sb       = new StringBuilder(32);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long partialTotal = 0L;
        long partialCount = 0L;

        for (Text token : values) {

            if (token == null) {
                context.getCounter(COUNTER_GROUP, "NULL_TOKEN_SKIPPED").increment(1);
                continue;
            }

            String s = token.toString().trim();

            if (s.isEmpty()) {
                context.getCounter(COUNTER_GROUP, "EMPTY_TOKEN_SKIPPED").increment(1);
                continue;
            }

            if (s.contains(DELIM)) {

                // ----------------------------------------------------------
                // Partial aggregate from a previous combiner pass
                // Format: "partialTotal,count"
                // Both components must be valid non-negative longs.
                // ----------------------------------------------------------
                String[] parts = s.split(DELIM, -1);

                if (parts.length != 2) {
                    context.getCounter(COUNTER_GROUP,
                            "MALFORMED_AGGREGATE_SKIPPED").increment(1);
                    continue;
                }

                try {
                    long t = Long.parseLong(parts[0].trim());
                    long c = Long.parseLong(parts[1].trim());

                    if (t < 0 || c < 0) {
                        context.getCounter(COUNTER_GROUP,
                                "NEGATIVE_AGGREGATE_SKIPPED").increment(1);
                        continue;
                    }

                    partialTotal += t;
                    partialCount += c;

                } catch (NumberFormatException e) {
                    context.getCounter(COUNTER_GROUP,
                            "INVALID_AGGREGATE_FORMAT_SKIPPED").increment(1);
                }

            } else {

                // ----------------------------------------------------------
                // Raw salary string from EmployeeMapper
                // Format: "5000" or "5000.5"
                // Cast to long after validation — consistent with reducer.
                // ----------------------------------------------------------
                try {
                    double raw = Double.parseDouble(s);

                    if (raw < 0) {
                        context.getCounter(COUNTER_GROUP,
                                "NEGATIVE_SALARY_SKIPPED").increment(1);
                        continue;
                    }

                    partialTotal += (long) raw;
                    partialCount += 1L;

                } catch (NumberFormatException e) {
                    context.getCounter(COUNTER_GROUP,
                            "INVALID_SALARY_FORMAT_SKIPPED").increment(1);
                }
            }
        }

        if (partialCount == 0L) {
            context.getCounter(COUNTER_GROUP,
                    "EMPTY_COMBINE_GROUPS_SKIPPED").increment(1);
            return;
        }

        // Emit "partialTotal,count"
        sb.setLength(0);
        sb.append(partialTotal).append(DELIM).append(partialCount);

        outKey.set(key);
        outValue.set(sb.toString());

        context.write(outKey, outValue);
        context.getCounter(COUNTER_GROUP, "PARTIAL_AGGREGATES_EMITTED").increment(1);
    }
}