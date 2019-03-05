import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.jdouglas.map.PercentChangeFemaleEmploymentMapper;
import com.revature.jdouglas.map.PercentFemaleGradsMapper;
import com.revature.jdouglas.reduce.PercentChangeFemaleEmploymentReducer;
import com.revature.jdouglas.reduce.PercentFemaleGradsReducer;
import com.revature.jdouglas.map.AverageIncreaseFemaleEdMapper;
import com.revature.jdouglas.reduce.AverageIncreaseFemaleEdReducer;
import com.revature.jdouglas.map.PercentChangeMaleEmploymentMapper;
import com.revature.jdouglas.reduce.PercentChangeMaleEmploymentReducer;


public class GenderStatsTest {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> pfgmapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> pfgreduceDriver;
	private MapDriver<LongWritable, Text, Text, DoubleWritable> aifemapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> aifereduceDriver;
	private MapDriver<LongWritable, Text, Text, DoubleWritable> percentChangeMaleMapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> percentChangeMaleReduceDriver;
	private MapDriver<LongWritable, Text, Text, DoubleWritable> percentChangeFemaleMapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> percentChangeFemaleReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper and reducer test harnesses.
		 */
		PercentFemaleGradsMapper pfgmapper = new PercentFemaleGradsMapper();
		pfgmapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		pfgmapDriver.setMapper(pfgmapper);

		PercentFemaleGradsReducer pfgreducer = new PercentFemaleGradsReducer();
		pfgreduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		pfgreduceDriver.setReducer(pfgreducer);
		
		
		AverageIncreaseFemaleEdMapper aifemapper = new AverageIncreaseFemaleEdMapper();
		aifemapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		aifemapDriver.setMapper(aifemapper);

		AverageIncreaseFemaleEdReducer aifereducer = new AverageIncreaseFemaleEdReducer();
		aifereduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		aifereduceDriver.setReducer(aifereducer);
		
		
		PercentChangeMaleEmploymentMapper pcmemapper = new PercentChangeMaleEmploymentMapper();
		percentChangeMaleMapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		percentChangeMaleMapDriver.setMapper(pcmemapper);

		PercentChangeMaleEmploymentReducer pcmereducer = new PercentChangeMaleEmploymentReducer();
		percentChangeMaleReduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		percentChangeMaleReduceDriver.setReducer(pcmereducer);
		
		PercentChangeFemaleEmploymentMapper pcfemapper = new PercentChangeFemaleEmploymentMapper();
		percentChangeFemaleMapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		percentChangeFemaleMapDriver.setMapper(pcfemapper);

		PercentChangeFemaleEmploymentReducer pcfereducer = new PercentChangeFemaleEmploymentReducer();
		percentChangeFemaleReduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		percentChangeFemaleReduceDriver.setReducer(pcfereducer);
	}
	
	@Test
	public void testPercentFemaleGradsMapper() {

		pfgmapDriver.withInput(new LongWritable(1), new Text("\"Croatia\",\"HRV\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.78288\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.2888\",\"\",\"\",\"\",\"\",\"\","));
		pfgmapDriver.withOutput(new Text("Croatia"), new DoubleWritable(12.78288));
		pfgmapDriver.withOutput(new Text("Croatia"), new DoubleWritable(18.2888));
		pfgmapDriver.runTest();
	}

	@Test
	public void testPercentFemaleGradsReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(12.78288));
		values.add(new DoubleWritable(18.2888));

		pfgreduceDriver.withInput(new Text("Croatia"), values);
		pfgreduceDriver.withOutput(new Text("Croatia"), new DoubleWritable(16.0));
		pfgreduceDriver.runTest();
	}

	@Test
	public void testAverageIncreaseFemaleEdMapper() {

		aifemapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(35.37453));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(36.00504));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(37.52263));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(38.44067));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(39.15297));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(39.89922));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(40.53132));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(41.12231));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(20.18248));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(20.38445));
		aifemapDriver.withOutput(new Text("United States"), new DoubleWritable(20.68499));
		aifemapDriver.runTest();
	}

	@Test
	public void testAverageIncreaseFemaleEdReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(35.37453));
		values.add(new DoubleWritable(36.00504));
		values.add(new DoubleWritable(37.52263));
		values.add(new DoubleWritable(38.44067));
		values.add(new DoubleWritable(39.15297));
		values.add(new DoubleWritable(39.89922));
		values.add(new DoubleWritable(40.53132));
		values.add(new DoubleWritable(41.12231));
		values.add(new DoubleWritable(20.18248));
		values.add(new DoubleWritable(20.38445));
		values.add(new DoubleWritable(20.68499));

		aifereduceDriver.withInput(new Text("United States"), values);
		aifereduceDriver.withOutput(new Text("United States"), new DoubleWritable(-1.0));
		aifereduceDriver.runTest();
	}
	
	@Test
	public void testPercentChangeMaleEmploymentMapper() {

		percentChangeMaleMapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"78.879997253418\",\"77.5599975585938\",\"77.7300033569336\",\"77.0999984741211\",\"77.2699966430664\",\"77.5100021362305\",\"77.8600006103516\",\"77.9599990844727\",\"77.8000030517578\",\"77.6100006103516\",\"76.1800003051758\",\"74.9000015258789\",\"75.0299987792969\",\"75.5500030517578\",\"74.879997253418\",\"71.7300033569336\",\"72.0400009155273\",\"72.7799987792969\",\"73.7600021362305\",\"73.8399963378906\",\"72.0199966430664\",\"71.2900009155273\",\"69.0199966430664\",\"68.8099975585938\",\"70.6800003051758\",\"70.9000015258789\",\"70.9700012207031\",\"71.4700012207031\",\"72.0199966430664\",\"72.4599990844727\",\"72.0400009155273\",\"70.3600006103516\",\"69.8399963378906\",\"70.0199966430664\",\"70.4300003051758\",\"70.7900009155273\",\"70.9000015258789\",\"71.3099975585938\",\"71.5800018310547\",\"71.6500015258789\",\"71.8899993896484\",\"70.870002746582\",\"69.7099990844727\",\"68.9000015258789\",\"69.1900024414063\",\"69.5999984741211\",\"70.0699996948242\",\"69.7600021362305\",\"68.5\",\"64.5500030517578\",\"63.689998626709\",\"63.8699989318848\",\"64.3899993896484\",\"64.4000015258789\",\"64.879997253418\",\"65.3399963378906\",\"65.7699966430664\","));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(71.8899993896484));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(70.870002746582));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(69.7099990844727));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(68.9000015258789));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(69.1900024414063));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(69.5999984741211));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(70.0699996948242));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(69.7600021362305));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(68.5));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(64.5500030517578));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(63.689998626709));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(63.8699989318848));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(64.3899993896484));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(64.4000015258789));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(64.879997253418));
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(65.3399963378906)); 
		percentChangeMaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(65.7699966430664)); 
		percentChangeMaleMapDriver.runTest();
	}

	@Test
	public void testPercentChangeMaleEmploymentReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(71.8899993896484));
		values.add(new DoubleWritable(70.870002746582));
		values.add(new DoubleWritable(69.7099990844727));
		values.add(new DoubleWritable(68.9000015258789));
		values.add(new DoubleWritable(69.1900024414063));
		values.add(new DoubleWritable(69.5999984741211));
		values.add(new DoubleWritable(70.0699996948242));
		values.add(new DoubleWritable(69.7600021362305));
		values.add(new DoubleWritable(68.5));
		values.add(new DoubleWritable(64.5500030517578));
		values.add(new DoubleWritable(63.689998626709));
		values.add(new DoubleWritable(63.8699989318848));
		values.add(new DoubleWritable(64.3899993896484));
		values.add(new DoubleWritable(64.4000015258789));
		values.add(new DoubleWritable(64.879997253418));
		values.add(new DoubleWritable(65.3399963378906)); 
		values.add(new DoubleWritable(65.7699966430664)); 
		
		percentChangeMaleReduceDriver.withInput(new Text("United States"), values);
		percentChangeMaleReduceDriver.withOutput(new Text("United States"), new DoubleWritable(-8.51));
		percentChangeMaleReduceDriver.runTest();
	}
	
	@Test
	public void testPercentChangeFemaleEmploymentMapper() {

		percentChangeFemaleMapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (national estimate)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"35.5200004577637\",\"35.3499984741211\",\"35.5699996948242\",\"35.8300018310547\",\"36.310001373291\",\"37.0900001525879\",\"38.3199996948242\",\"38.9900016784668\",\"39.6199989318848\",\"40.7099990844727\",\"40.7900009155273\",\"40.3600006103516\",\"40.9700012207031\",\"42.0499992370605\",\"42.5800018310547\",\"42.0299987792969\",\"43.2299995422363\",\"44.4799995422363\",\"46.3699989318848\",\"47.4599990844727\",\"47.6699981689453\",\"47.9799995422363\",\"47.6699981689453\",\"48.0400009155273\",\"49.4900016784668\",\"50.4199981689453\",\"51.3800010681152\",\"52.5099983215332\",\"53.4300003051758\",\"54.310001373291\",\"54.3499984741211\",\"53.689998626709\",\"53.7599983215332\",\"54.0999984741211\",\"55.25\",\"55.6300010681152\",\"56.0400009155273\",\"56.7999992370605\",\"57.0800018310547\",\"57.4300003051758\",\"57.4900016784668\",\"57\",\"56.2700004577637\",\"56.1300010681152\",\"55.9700012207031\",\"56.2400016784668\",\"56.6199989318848\",\"56.6399993896484\",\"56.25\",\"54.4199981689453\",\"53.5699996948242\",\"53.189998626709\",\"53.1300010681152\",\"53.1599998474121\",\"53.5200004577637\",\"53.7400016784668\",\"54.0800018310547\","));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(57.4900016784668));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(57));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.2700004577637));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.1300010681152));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(55.9700012207031));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.2400016784668));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.6199989318848));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.6399993896484));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(56.25));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(54.4199981689453));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.5699996948242));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.189998626709));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.1300010681152));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.1599998474121));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.5200004577637));
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(53.7400016784668)); 
		percentChangeFemaleMapDriver.withOutput(new Text("United States"), new DoubleWritable(54.0800018310547)); 
		percentChangeFemaleMapDriver.runTest();
	}

	@Test
	public void testPercentChangeFemaleEmploymentReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(57.4900016784668));
		values.add(new DoubleWritable(57));
		values.add(new DoubleWritable(56.2700004577637));
		values.add(new DoubleWritable(56.1300010681152));
		values.add(new DoubleWritable(55.9700012207031));
		values.add(new DoubleWritable(56.2400016784668));
		values.add(new DoubleWritable(56.6199989318848));
		values.add(new DoubleWritable(56.6399993896484));
		values.add(new DoubleWritable(56.25));
		values.add(new DoubleWritable(54.4199981689453));
		values.add(new DoubleWritable(53.5699996948242));
		values.add(new DoubleWritable(53.189998626709));
		values.add(new DoubleWritable(53.1300010681152));
		values.add(new DoubleWritable(53.1599998474121));
		values.add(new DoubleWritable(53.5200004577637));
		values.add(new DoubleWritable(53.7400016784668)); 
		values.add(new DoubleWritable(54.0800018310547)); 
		
		percentChangeFemaleReduceDriver.withInput(new Text("United States"), values);
		percentChangeFemaleReduceDriver.withOutput(new Text("United States"), new DoubleWritable(-5.93));
		percentChangeFemaleReduceDriver.runTest();
	}
}
