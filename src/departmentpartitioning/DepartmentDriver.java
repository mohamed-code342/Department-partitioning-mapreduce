package departmentpartitioning;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * DepartmentDriver
 *
 * Configures and submits the DepartmentPartitioning MapReduce job.
 *
 * Usage:
 *   hadoop jar DepartmentPartitioning.jar \
 *          departmentpartitioning.DepartmentDriver \
 *          <inputPath> <outputPath>
 *
 * Exit codes: 0 = success, 1 = failure.
 */
public class DepartmentDriver extends Configured implements Tool {

    /** One reducer per known department in DepartmentPartitioner. */
    private static final int NUM_REDUCERS = 4;

    @Override
    public int run(String[] args) throws Exception {

        // ------------------------------------------------------------------
        // Argument validation
        // ------------------------------------------------------------------
        if (args == null || args.length < 2) {
            System.err.println("Usage: DepartmentDriver <inputPath> <outputPath>");
            return 1;
        }

        String inputPath  = args[0].trim();
        String outputPath = args[1].trim();

        if (inputPath.isEmpty() || outputPath.isEmpty()) {
            System.err.println("ERROR: inputPath and outputPath must not be blank.");
            return 1;
        }

        // ------------------------------------------------------------------
        // Job construction
        // ------------------------------------------------------------------
        Job job = Job.getInstance(getConf(),
                "DepartmentPartitioning - Employee Salary Analysis");

        // JAR registration — distributes all classes to every node
        job.setJarByClass(DepartmentDriver.class);

        // ------------------------------------------------------------------
        // Component registration
        // ------------------------------------------------------------------
        job.setMapperClass(EmployeeMapper.class);
        job.setCombinerClass(SalaryCombiner.class);
        job.setPartitionerClass(DepartmentPartitioner.class);
        job.setReducerClass(SalaryReducer.class);

        // ------------------------------------------------------------------
        // Reducer count
        // Must match or exceed the highest partition index in the routing
        // table so that every department lands in a real reducer slot.
        // ------------------------------------------------------------------
        job.setNumReduceTasks(NUM_REDUCERS);

        // ------------------------------------------------------------------
        // Map output types (declared explicitly — never rely on defaults)
        // ------------------------------------------------------------------
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // ------------------------------------------------------------------
        // Final output types
        // ------------------------------------------------------------------
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ------------------------------------------------------------------
        // Input / output formats
        // ------------------------------------------------------------------
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // ------------------------------------------------------------------
        // Paths
        // ------------------------------------------------------------------
        FileInputFormat.addInputPath(job,  new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // ------------------------------------------------------------------
        // Submission
        // ------------------------------------------------------------------
        System.out.println("Input  : " + inputPath);
        System.out.println("Output : " + outputPath);
        System.out.println("Reducers: " + NUM_REDUCERS);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) {
        int exit;
        try {
            exit = ToolRunner.run(new DepartmentDriver(), args);
        } catch (Exception e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace(System.err);
            exit = 1;
        }
        System.exit(exit);
    }
}