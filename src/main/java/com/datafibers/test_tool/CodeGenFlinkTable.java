package com.datafibers.test_tool;
import net.openhft.compiler.CompilerUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;

import com.datafibers.util.DynamicRunner;

/**
 * The goal is to define a function with parameter TABLE, Transformation Script and return TABLE
 * This function will dynamically generate code and run.
 */
public class CodeGenFlinkTable {

	public static Table getFlinkTableObj(String className, String javaCode){
		try {
			Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
			DynamicRunner runner = (DynamicRunner) aClass.newInstance();
			//return runner.getTableObj();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[]) {

		String transform = "flatMap(new FlinkUDF.LineSplitter()).groupBy(0).sum(1).print();\n";

		String transform2 = "select(\"name\");\n";

		String header = "package dynamic;\n" +
				"import org.apache.flink.api.table.Table;\n" +
				"import com.datafibers.util.*;\n";

		String javaCode = header +
				"public class FlinkScript implements DynamicRunner {\n" +
				"@Override \n" +
				"    public void runTransform(DataSet<String> ds) {\n" +
						"try {" +
						"ds."+ transform +
						"} catch (Exception e) {" +
						"};" +
				"}}";

		String javaCode2 = header +
				"public class FlinkScript implements DynamicRunner {\n" +
				"@Override \n" +
				"    public Table transTableObj(Table tbl) {\n" +
					"try {" +
					"return tbl."+ transform2 +
					"} catch (Exception e) {" +
					"};" +
					"return null;}}";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		CsvTableSource csvTableSource = new CsvTableSource(
				"/Users/will/Downloads/file.csv",
				new String[] { "name", "id", "score", "comments" },
				new TypeInformation<?>[] {
						Types.STRING(),
						Types.STRING(),
						Types.STRING(),
						Types.STRING()
				}); // lenient

		tableEnv.registerTableSource("mycsv", csvTableSource);
		TableSink sink = new CsvTableSink("/Users/will/Downloads/out.csv", "|");
		Table ingest = tableEnv.scan("mycsv");

		try {
			String className = "dynamic.FlinkScript";
			Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode2);
			DynamicRunner runner = (DynamicRunner) aClass.newInstance();
			//runner.runTransform(ds);
			Table result = runner.transTableObj(ingest);
			// write the result Table to the TableSink
			result.writeToSink(sink);
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}