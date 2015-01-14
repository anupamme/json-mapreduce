/*
 * Copyright 2013 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alexholmes.json.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.*;

import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.List;


/**
 * An example MapReduce job showing how to use the {@link com.alexholmes.json.mapreduce.MultiLineJsonInputFormat}.
 */
public final class SecondExampleJobWithDistributedCache extends Configured implements Tool {

	/**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
	
	public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SecondExampleJobWithDistributedCache(), args);
        System.exit(res);
    }

	private static void addJarToDistributedCache(
	        Class classToAdd, Configuration conf)
	    throws IOException {
	 
	    // Retrieve jar file for class2Add
	    String jar = classToAdd.getProtectionDomain().
	            getCodeSource().getLocation().
	            getPath();
	    File jarFile = new File(jar);
	 
	    // Declare new HDFS location
	    Path hdfsJar = new Path("/Users/mediratta/Documents/workspace/json-mapreduce/hdfs-new/"
	            + jarFile.getName());
	 
	    // Mount HDFS
	    FileSystem hdfs = FileSystem.get(conf);
	 
	    // Copy (override) jar file to HDFS
	    hdfs.copyFromLocalFile(false, true,
	        new Path(jar), hdfsJar);
	 
	    // Add jar to distributed classPath
	    DistributedCache.addFileToClassPath(hdfsJar, conf);
	}
	
    /**
     * The MapReduce driver - setup and launch the job.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

    	String input = args[0];
        String output = args[1];

        Configuration conf = super.getConf();
//        conf.set("mapred.child.java.opts", "4g");
     // Add 3rd-party libraries
        addJarToDistributedCache(edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.ling.CoreLabel.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.pipeline.StanfordCoreNLP.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.pipeline.Annotation.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.sentiment.SentimentCoreAnnotations.class, conf);
        addJarToDistributedCache(edu.stanford.nlp.util.CoreMap.class, conf);
        
////        writeInput(conf, new Path(input));
        
        
        String basePath = "/Users/mediratta/Documents/workspace/json-mapreduce/";
        String hadoop_classpath = basePath + "stanford-jars/ejml-0.23.jar"
        		+ basePath + ":stanford-jars/stanford-corenlp-3.4.1-models.jar"
        		+ basePath + ":stanford-jars/stanford-corenlp-3.4.1-sources.jar"
        		+ basePath + ":stanford-jars/stanford-corenlp-3.4.1.jar";
        Job job = new Job(conf, hadoop_classpath);
//        Job job = new Job(conf);
        
        //DistributedCache.addCacheFile(new URI("/cachefile1"), conf);
        
        job.setJarByClass(SecondExampleJobWithDistributedCache.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(0);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        // use the JSON input format
        job.setInputFormatClass(MultiLineJsonInputFormat.class);
        //job.setOutputFormatClass(MultiLineJsonInputFormat.class);
        
        // specify the JSON attribute name which is used to determine which
        // JSON elements are supplied to the mapper
        MultiLineJsonInputFormat.setInputJsonMember(job, "hotelid");

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
    	@Override
    	protected void reduce(LongWritable key, Iterable<Text> values, Context context) 
    			throws IOException, InterruptedException{
    		context.write(new Text("in reducer"), null);
    		JSONObject res = new JSONObject();
    		for (Text val : values){
    			context.write(new Text("checking value: " + val), null);
    			JSONObject arg = new JSONObject(val.toString());
    			Iterator<String> allkeys = arg.keys();
    			while(allkeys.hasNext()){
    				String hotelid = allkeys.next();
    				res.put(hotelid, arg.getJSONArray(hotelid));
    			}
    		}
    		context.write(new Text(key.toString()), new Text(res.toString()));
    	}
    }
    
    /**
     * JSON objects are supplied in string form to the mapper.
     * Here we are simply emitting them for viewing on HDFS.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    	public Map(){

    	}
    	
    	// pipeline options
    	private static final Properties PROPS = new Properties();

    	static {
    		PROPS.put("annotators", "tokenize, cleanxml, ssplit, pos, lemma, ner, parse, dcoref,sentiment");
    	}
    	
    	// create pipeline
    	private final StanfordCoreNLP pipeline = new StanfordCoreNLP(PROPS);

    	
    	@Override 
    	protected void setup(Context context) throws IOException, InterruptedException {
    		
    	}
    	
    	@Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	
    		
    		
    		JSONObject obj = new JSONObject(value);
            JSONArray arr = (JSONArray) obj.get("bytes");
            // force convert jsonarry to bytes array.
            byte[] dest = new byte[arr.length()];
            for (int val = 0; val < arr.length(); val++){
            	dest[val] = (byte) arr.get(val);
            }
            String reviewStr = new String(dest, "UTF-8");
            System.out.println("processing the review:" + reviewStr);
            //context.write(new Text(String.format("processing the review: '%s'", reviewStr)), null);
            JSONObject obj2 = new JSONObject(reviewStr);
            String hotelId = obj2.getString("hotelid");
            String location = obj2.getString("location");
            
            JSONArray reviewArr = obj2.getJSONArray("reviewList");
            JSONArray processedData = processReview(reviewArr);
//            String review = new String((byte[])obj.get("bytes"), "UTF-8");

            // emit the tuple and the original contents of the line
            JSONObject res = new JSONObject();
            //res.append("hotelId", hotelId);
            res.put(hotelId, processedData);
            context.write(new Text(location), new Text(res.toString()));
//            context.write(new Text(String.format("%s", res.toString())), null);
            //context.write(new Text(String.format("Got value: '%s'", formatted)), null);
        }
    	
    	private JSONArray processReview(JSONArray reviewList){
    		JSONArray obj = new JSONArray();
    		for (int i = 0; i < reviewList.length(); i++){
    			JSONObject reviewObj = (JSONObject) reviewList.get(i);
    			if (!reviewObj.has("review")){
    				continue;
    			}
    			String value2 = (String) reviewObj.get("review");
    			String text = value2; // Add your text here!
        		// create an empty Annotation just with the given text
        		Annotation document = new Annotation(text);

        		// run all Annotators on this text
        		pipeline.annotate(document);
        			    
        		// these are all the sentences in this document
        		// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        		
        			
        		
        		Object[] senArr = new Object[sentences.size()];
        		int highercount = 0;
        		for(CoreMap sentence: sentences) {
        			JSONObject senMap = new JSONObject();
        			String sentiment = sentence.get(SentimentCoreAnnotations.ClassName.class);

        			senMap.put("sentiment", sentiment);
        			
        			// traversing the words in the current sentence
        			// a CoreLabel is a CoreMap with additional token-specific methods
        			List<CoreLabel> labelArr = sentence.get(TokensAnnotation.class);
        			Object[] data = new Object[labelArr.size()];
        			int count = 0;
        			for (CoreLabel token: labelArr) {
        				JSONObject labelMap = new JSONObject();
            			String word = token.get(TextAnnotation.class);
        			    labelMap.put("word", word);
        			    String pos = token.get(PartOfSpeechAnnotation.class);
        			    labelMap.put("pos", pos);
        			    data[count++] = labelMap;
        			}
        			senMap.put("worddetails", data);
        			senArr[highercount++] = senMap;
        		}
        		obj.put(senArr);
    		}
    		
			return obj;
    }
    	
    }
    
}
