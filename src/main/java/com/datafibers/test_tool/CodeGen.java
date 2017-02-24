package com.datafibers.test_tool;
import com.datafibers.test_tool.FlinkUDF;
import net.openhft.compiler.CompilerUtils;

public class CodeGen {

	public static void main(String args[]) {
		String udfPackage = "com.datafibers.test_tool.FlinkUDF.";
		String className = "dynamic.WordCount";
		String article = 	"\"To be, or not to be,--that is the question:--" +
							"Whether 'tis nobler in the mind to suffer" +
							"The slings and arrows of outrageous fortune" +
							"Or to take arms against a sea of troubles,\"";
		String header = "package dynamic;\n" +
				"import org.apache.flink.api.java.DataSet;\n" +
				"import org.apache.flink.api.java.ExecutionEnvironment;\n" +
				"import org.apache.flink.api.common.functions.FlatMapFunction;\n" +
				"import org.apache.flink.api.java.tuple.Tuple2;\n" +
				"import org.apache.flink.util.Collector;";

		String javaCode = header +
				"public class WordCount implements Runnable {\n" +
				"    public void run() {\n" +
				"       final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
				"		DataSet<String> text = env.fromElements(" + article + ");\n" +
						"DataSet<Tuple2<String, Integer>> counts = " +
						"text.flatMap(new " + udfPackage + "LineSplitter()).groupBy(0).sum(1);\n" +
						"try {" +
						"counts.print();" +
						"} catch (Exception e) {" +
						"};" +
				"}}";

		try {
			Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
			Runnable runner = (Runnable) aClass.newInstance();
			runner.run();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}