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

import org.json.*;

//import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
//import edu.stanford.nlp.ling.CoreLabel;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import edu.stanford.nlp.trees.Tree;
//import edu.stanford.nlp.util.CoreMap;

//import java.awt.print.Printable;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Properties;
//import java.util.List;


/**
 * An example MapReduce job showing how to use the {@link com.alexholmes.json.mapreduce.MultiLineJsonInputFormat}.
 */
public final class ThirdExampleJob {

	/**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
	
//	static StanfordCoreNLP pipeline;
//	
//	public static void main(final String[] args) throws Exception {
//		
////		String[] javaStr = {"java", "-cp \"*\" -Xmx4g edu.stanford.nlp.pipeline.StanfordCoreNLP "
////        		+ "-annotators tokenize,ssplit,pos,lemma,ner,parse,dcoref,sentiment -file /Users/mediratta/Documents/workspace/json-mapreduce/data/"};
////		
////		Process proc = Runtime.getRuntime().exec(javaStr);
////		System.out.println(proc.getOutputStream());
////		proc.waitFor();
////		
////		System.out.println("I am done!");
//		
//		
//		String input = args[0];
//        
//        Properties props = new Properties();
//        props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
//        pipeline = new StanfordCoreNLP(props);
//        
//        JSONObject obj = processReview(input);
//    }
//
//    	
//    	private static JSONObject processReview(String value){
//    		String text = value; // Add your text here!
//    		// create an empty Annotation just with the given text
//    		Annotation document = new Annotation(text);
//
//    		// run all Annotators on this text
//    		pipeline.annotate(document);
//    			    
//    		// these are all the sentences in this document
//    		// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
//    		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
//    		
//    		JSONObject obj = new JSONObject();
//    		Object[] senArr = new Object[sentences.size()];
//    		int highercount = 0;
//    		for(CoreMap sentence: sentences) {
//    			JSONObject senMap = new JSONObject();
//    			String sentiment = sentence.get(SentimentCoreAnnotations.ClassName.class);
//
//    			senMap.put("sentiment", sentiment);
//    			
//    			// traversing the words in the current sentence
//    			// a CoreLabel is a CoreMap with additional token-specific methods
//    			List<CoreLabel> labelArr = sentence.get(TokensAnnotation.class);
//    			Object[] data = new Object[labelArr.size()];
//    			int count = 0;
//    			for (CoreLabel token: labelArr) {
//    				JSONObject labelMap = new JSONObject();
//        			String word = token.get(TextAnnotation.class);
//    			    labelMap.put("word", word);
//    			    String pos = token.get(PartOfSpeechAnnotation.class);
//    			    labelMap.put("pos", pos);
//    			    data[count++] = labelMap;
//    			}
//    			senMap.put("worddetails", data);
//    			senArr[highercount++] = senMap;
//    		}
//			obj.put("sentences", senArr);
//    			    return obj;
//    }
}
