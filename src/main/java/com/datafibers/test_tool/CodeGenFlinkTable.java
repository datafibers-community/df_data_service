package com.datafibers.test_tool;
import net.openhft.compiler.CompilerUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;

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

		String header = "package dynamic;\n" +
				"import org.apache.flink.api.java.DataSet;\n" +
				"import org.apache.flink.api.java.ExecutionEnvironment;\n" +
				"import org.apache.flink.api.common.functions.FlatMapFunction;\n" +
				"import org.apache.flink.api.java.tuple.Tuple2;\n" +
				"import com.datafibers.test_tool.*;\n" +
				"import org.apache.flink.util.Collector;";

		String javaCode = header +
				"public class FlinkScript implements DynamicRunner {\n" +
				"@Override \n" +
				"    public void runTransform(DataSet<String> ds) {\n" +
						"try {" +
						"ds."+ transform +
						"} catch (Exception e) {" +
						"};" +
				"}}";

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> ds = env.fromElements("test a a a the the");

		try {
			String className = "dynamic.FlinkScript";
			Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
			DynamicRunner runner = (DynamicRunner) aClass.newInstance();
			runner.runTransform(ds);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}