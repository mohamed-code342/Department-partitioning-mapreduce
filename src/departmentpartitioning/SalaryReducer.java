package departmentpartitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SalaryReducer
 *
 * Receives all values for one department (after shuffle + sort) and emits
 * one summary line containing total salary, average salary, and headcount.
 *
 * VALUE STREAM CONTRACT
 * ─────────────────────
 * Because the combiner may run 0, 1, or N times, the value stream is a
 * mixture of:
 *
 *   "5000"      raw salary string  (combiner did not run for this record)
 *   "15800,3"   partial aggregate  (combiner consolidated N records)
 *
 * Detection: presence of "," → partial aggregate; absence → raw salary.
 * This is safe because EmployeeMapper guarantees salary strings are plain
 * numerics and therefore never contain a comma.
 *
 * Output format:
 *   IT    Total: 15800    Avg: 5266    Employees: 3
 */
public class SalaryReducer extends Reducer<Text, Text, Text, Text> {

    private static final String DELIM         = ",";
    private static final String TAB           = "\t";
    private static final String COUNTER_GROUP = "SalaryReducer.DataQuality";

    private final Text          outKey   = new Text();
    private final Text          outValue = new Text();
    private final StringBuilder sb       = new StringBuilder(64);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalSalary   = 0L;
        long employeeCount = 0L;

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

            // ----------------------------------------------------------------
            // BRANCH DETECTION
            // A comma means this token is a partial aggregate from the combiner.
            // No comma means this is a raw salary string from the mapper.
            // ----------------------------------------------------------------
            if (s.contains(DELIM)) {

                // ------------------------------------------------------------
                // Branch A — partial aggregate: "partialTotal,count"
                // ------------------------------------------------------------
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

                    totalSalary   += t;
                    employeeCount += c;

                } catch (NumberFormatException e) {
                    // This fires if the combiner emitted a malformed token.
                    // Surface it as a named counter — never swallow silently.
                    context.getCounter(COUNTER_GROUP,
                            "INVALID_AGGREGATE_FORMAT_SKIPPED").increment(1);
                }

            } else {

                // ------------------------------------------------------------
                // Branch B — raw salary: "5000" or "5000.5"
                // ------------------------------------------------------------
                try {
                    double raw = Double.parseDouble(s);

                    if (raw < 0) {
                        context.getCounter(COUNTER_GROUP,
                                "NEGATIVE_SALARY_SKIPPED").increment(1);
                        continue;
                    }

                    totalSalary   += (long) raw;
                    employeeCount += 1L;

                } catch (NumberFormatException e) {
                    context.getCounter(COUNTER_GROUP,
                            "INVALID_SALARY_FORMAT_SKIPPED").increment(1);
                }
            }
        }

        // ----------------------------------------------------------------
        // Guard — no valid records: emit nothing rather than a zero row.
        // If this counter is non-zero after the job, investigate upstream.
        // ----------------------------------------------------------------
        if (employeeCount == 0L) {
            context.getCounter(COUNTER_GROUP,
                    "DEPARTMENTS_WITH_NO_VALID_RECORDS").increment(1);
            return;
        }

        // ----------------------------------------------------------------
        // Average — integer truncation per project specification.
        // Division by zero excluded by guard above.
        // ----------------------------------------------------------------
        long avg = totalSalary / employeeCount;

        // ----------------------------------------------------------------
        // Output formatting
        // ----------------------------------------------------------------
        sb.setLength(0);
        sb.append("Total: ")     .append(totalSalary)
          .append(TAB)
          .append("Avg: ")       .append(avg)
          .append(TAB)
          .append("Employees: ") .append(employeeCount);

        outKey.set(key);
        outValue.set(sb.toString());

        context.write(outKey, outValue);
        context.getCounter(COUNTER_GROUP, "DEPARTMENTS_EMITTED").increment(1);
    }
}