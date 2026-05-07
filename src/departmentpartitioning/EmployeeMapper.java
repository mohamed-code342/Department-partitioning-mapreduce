package departmentpartitioning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * EmployeeMapper
 *
 * Parses CSV lines and emits (department, salaryString).
 * The salary is emitted as the original trimmed string — never reformatted —
 * so downstream parsing is unambiguous.
 *
 * Input  : "E001,IT,5000,Developer,2020-01-15"
 * Output : ("IT", "5000")
 */
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int    EXPECTED_FIELDS  = 5;
    private static final int    IDX_DEPARTMENT   = 1;
    private static final int    IDX_SALARY       = 2;
    private static final String COUNTER_GROUP    = "EmployeeMapper.DataQuality";

    // Reusable output objects — never recreated inside map()
    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // ----------------------------------------------------------------
        // 1. Raw line extraction
        // ----------------------------------------------------------------
        String line = value.toString().trim();

        if (line.isEmpty()) {
            context.getCounter(COUNTER_GROUP, "EMPTY_LINES_SKIPPED").increment(1);
            return;
        }

        // ----------------------------------------------------------------
        // 2. Field splitting
        //    limit=-1 preserves trailing empty fields so field-count check
        //    catches records like "E001,IT,5000,Developer,"
        // ----------------------------------------------------------------
        String[] fields = line.split(",", -1);

        if (fields.length != EXPECTED_FIELDS) {
            context.getCounter(COUNTER_GROUP, "WRONG_FIELD_COUNT_SKIPPED").increment(1);
            return;
        }

        // ----------------------------------------------------------------
        // 3. Field extraction with whitespace trim
        //    Trim BEFORE validation — CSV files often have spaces after commas
        // ----------------------------------------------------------------
        String department = fields[IDX_DEPARTMENT].trim();
        String salaryRaw  = fields[IDX_SALARY].trim();

        if (department.isEmpty()) {
            context.getCounter(COUNTER_GROUP, "MISSING_DEPARTMENT_SKIPPED").increment(1);
            return;
        }

        if (salaryRaw.isEmpty()) {
            context.getCounter(COUNTER_GROUP, "MISSING_SALARY_SKIPPED").increment(1);
            return;
        }

        // ----------------------------------------------------------------
        // 4. Salary numeric validation
        //    Parse to validate, but emit the original trimmed string.
        //    This avoids reformatting (e.g. 5000 → "5000.0") that could
        //    confuse downstream parsing.
        // ----------------------------------------------------------------
        double salary;
        try {
            salary = Double.parseDouble(salaryRaw);
        } catch (NumberFormatException e) {
            context.getCounter(COUNTER_GROUP, "NON_NUMERIC_SALARY_SKIPPED").increment(1);
            return;
        }

        if (salary < 0) {
            context.getCounter(COUNTER_GROUP, "NEGATIVE_SALARY_SKIPPED").increment(1);
            return;
        }

        // ----------------------------------------------------------------
        // 5. Emit (department, salaryString)
        //    Key   = department  → drives partitioner and reducer grouping
        //    Value = salaryRaw   → original string, no reformatting
        // ----------------------------------------------------------------
        outKey.set(department);
        outValue.set(salaryRaw);

        context.write(outKey, outValue);
        context.getCounter(COUNTER_GROUP, "RECORDS_EMITTED").increment(1);
    }
}