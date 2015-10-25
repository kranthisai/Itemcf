package ItemBasedCf.Itemcf;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import Jama.Matrix;

import scala.Tuple2;

public class CollaborativeFiltering 
{
    public static void main( String[] args )
    {
        
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path ="u.data";
        JavaRDD<String> data = sc.textFile(path);
       
       
        final List<Integer> movies = data.map(
      	      new Function<String, Integer>() {
      	        public Integer call(String s) {
      	          String[] sarray = s.split("\t");
      	          return new Integer(sarray[1]);
      	        }
      	      }
      	    ).repartition(1).distinct().collect();
  	  Collections.sort(movies);
  	  
  	  final List<Integer> users = data.map(
    	      new Function<String, Integer>() {
    	        public Integer call(String s) {
    	          String[] sarray = s.split("\t");
    	          return new Integer(sarray[0]);
    	        }
    	      }
    	    ).repartition(1).distinct().collect();
	  // Creating RDD. It is of the form <userid,<itemid,rating>> userid is key 
	  JavaPairRDD<Integer, Tuple2<Integer, Double>> ratings = data.mapToPair(
		        new PairFunction<String, Integer,Tuple2<Integer, Double>>() {
		          public Tuple2<Integer,Tuple2<Integer, Double>> call(String r){
		        	  String[] record = r.split("\t");
		        	  Double rating=Double.parseDouble(record[2]);
		        	  Integer userid=Integer.parseInt(record[0]);
		        	  Integer movieid=Integer.parseInt(record[1]);
		        	  return new Tuple2<Integer, Tuple2<Integer,Double>>(userid,new Tuple2<Integer,Double>(movieid,rating));
		          }
		        }
		        ).repartition(1).sortByKey(); 
       
      final  int usersSize = users.size();
      final  int m = users.size();
      final int moviesSize = movies.size();
      final int n=movies.size();
        
        JavaPairRDD<Integer,List<Double>> utility= ratings.mapPartitionsToPair
  			  (new PairFlatMapFunction<Iterator<Tuple2<Integer,Tuple2<Integer,Double>>>,Integer,List<Double>>()
  			  {
  		  		
  				public Iterable<Tuple2<Integer, List<Double>>> call(
  						Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> s)
  						throws Exception {
  					// TODO Auto-generated method stub
  				List<Double> userratings=new ArrayList<Double>();
  				List<Tuple2<Integer, List<Double>>> utilmatrix=new ArrayList<Tuple2<Integer, List<Double>>>();
  				
  				
  					while(s.hasNext())
  					{
  						userratings=new ArrayList<Double>();
  						for (int i=0;i<movies.size();i++)
  						{
  							userratings.add((double) 0);
  						}
  						Tuple2<Integer, Tuple2<Integer, Double>> t=s.next();
  						int userid=t._1;
  						int moviesid=t._2._1;
  						Double rating=t._2._2;
  						int index=movies.indexOf(moviesid);
  						//System.out.println(index);
  						userratings.set(index, rating);
  						//System.out.println(userratings);
  						while(s.hasNext() )
  						{
  							t=s.next();
  							if(t._1==userid)
  							{
  								moviesid=t._2._1;
  								rating=t._2._2;
  								index=movies.indexOf(moviesid);
  								userratings.set(index, rating);
  							}
  							else
  								break;
  						
  						}
  						Tuple2<Integer,List<Double>> t1 = new Tuple2<Integer,List<Double>>(userid,userratings) ;
  				
  						utilmatrix.add(t1);
  					
  					}	
  					return utilmatrix;
  				}
  			  }
  			  
  			  );
        // Uncomment the following line, if you want to see userid,ratings for each item he rated.
       utility.saveAsTextFile("utility");
        
  
    	// Finding item item cosine similarity between every pair of items
    	
    	JavaRDD<List<Double>> cosinematrix=utility.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,List<Double>>>,List<Double>>(){

    		public Iterable<List<Double>> call(
    				Iterator<Tuple2<Integer, List<Double>>> t) throws Exception {
    			// TODO Auto-generated method stub
    		
    			int i=0;
    	
    			List<Double> cosimilarlist=new ArrayList<Double>();
    			List<List<Double>> cosinesimilarity=new ArrayList<List<Double>>();
    			Matrix utils1=new Matrix(m,n);
    			Matrix cosine=new Matrix(n,n);
    			double[][] y = null,y1=null;
    			while(t.hasNext())
    			{
    				Tuple2<Integer, List<Double>> temp1=t.next();
    				List<Double> lis=temp1._2;
    				for(int j=0;j<lis.size();j++)
    				{
    					utils1.set(i, j,lis.get(j));
    				}
    				i++;
    			}
    			double[] d =new double[m];
    			double[] e= new double[m];
    			for(int k=0;k<n;k++)
    			{
    				for(int j=0;j<m;j++)
    				{
    					double d1=utils1.get(j,k);
    					//System.out.println(d1);
    					d[j]= d1;
    				}
    				for(int k1=k+1;k1<n;k1++)
    				{
    					double num=0;
    					double d2=0;
    					double e2=0;
    					for(int g=0;g<m;g++)
    					{
    						e[g]=utils1.get(g,k1);
    					}
    					for(int w=0;w<m;w++)
    					{
    						num+=d[w]*e[w];
    						d2+=d[w]*d[w];
    						e2+=e[w]*e[w];	
    					}
    					double result;
    					//double result=CosineDistanceMeasure.distance(d,e);
    					if((d2==0 || e2==0))
    					result=0.0;
    					else
    					result=(num)/(Math.sqrt(e2)*Math.sqrt(d2)); 
    					cosine.set(k, k1, result);
    					cosine.set(k1,k,result);
    				}
    			}
    			utils1=null;
    			y1=cosine.getArray();
    			for(int p=0;p<n;p++)
    			{
    				 cosimilarlist=new ArrayList<Double>();
    				 for(int f=0;f<n;f++)
    				 {
    			
    					 cosimilarlist.add(y1[p][f]);
    				 }
    				 cosinesimilarity.add(cosimilarlist);
    			}
    			
    			cosine=null;
    			
    			return cosinesimilarity;
    			
    		}
    		
    	});
    	
    	final List<List<Double>> cosines=cosinematrix.collect(); 
    	JavaPairRDD<Integer,List<Tuple2<Integer,Double>>> prediction=utility.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,List<Double>>>,Integer,List<Tuple2<Integer,Double>>>(){

			public Iterable<Tuple2<Integer, List<Tuple2<Integer, Double>>>> call(
					Iterator<Tuple2<Integer, List<Double>>> t) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<Integer, List<Tuple2<Integer, Double>>>> weightedAvgPrediction=new ArrayList<Tuple2<Integer, List<Tuple2<Integer, Double>>>>();
				while(t.hasNext())
				{
					Tuple2<Integer, List<Double>> userrating = t.next();
					Integer userid = userrating._1;
					List<Double> ratinglist = userrating._2;
					List<Integer> predictedmovies=new ArrayList<Integer>();
					List<Double> predictedratings=new ArrayList<Double>();
					List<List<Double>> finalmovies=new ArrayList<List<Double>>();
					List<Double> ratingpredict= new ArrayList<Double>();
					for(int i=0;i<ratinglist.size();i++)
					{
						// predicting value of items/movies for which user is not rated
						if((ratinglist.get(i)==0))
						{
							int movieid=movies.get(i);
							List<Double> similarmoviesdist = cosines.get(i);
							List<Double> temp= cosines.get(i);
							
							double weightedsum=0.0;
							double numsum = 0.0;
							double denumsum = 0.0;	
							//System.out.println("before"+temp);
							Collections.sort(temp ,Collections.reverseOrder());
							//System.out.println("after"+temp);
							for(int limit=0;limit<200&& limit<moviesSize;limit++)
							{
								// 0 if user is not rated for that movie. Note: This default value depends on range of ratings 
								Double userrated_movie = ratinglist.get(similarmoviesdist.indexOf(temp.get(limit)));
								numsum+=userrated_movie*temp.get(limit);
								denumsum+=temp.get(limit);
								
							}
							if(denumsum!=0)
								 weightedsum=numsum/denumsum;
							ratingpredict=new ArrayList<Double>();
							ratingpredict.add((double) movieid);
							ratingpredict.add(weightedsum);
							finalmovies.add(ratingpredict);
							
							
						}
					}
					Collections.sort(finalmovies, new Comparator<List<Double>>() {

					      public int compare(List<Double> o1, List<Double> o2) {
					              // TODO Auto-generated method stub				            	
					               if((o1.get(1)) <  (o2.get(1)))
					              	 return 1;
					               else if((o1.get(1))>(o2.get(1)))
					              		 return -1;
					               else
					              	 return 0;
					      }
						});
					
					// Return Top N Recommendations, Suppose you want 10 Recommendations for the user
					List<Tuple2<Integer, Double>> temp2=new ArrayList<Tuple2<Integer, Double>>();
					for(int j=0;j<10 && j<finalmovies.size();j++)
					{
						temp2.add(new Tuple2<Integer, Double>(finalmovies.get(j).get(0).intValue(),finalmovies.get(j).get(1)));
						//weightedAvgPrediction.add(new Tuple2<Integer, List<Tuple2<Integer, Double>>>(userid,finalmovies.get(j).get(0),finalmovies.get(j).get(1));
					}
					//Tuple2<Integer, List<Tuple2<Integer, Double>>> t2=new Tuple2<Integer, List<Tuple2<Integer, Double>>>()
					
					weightedAvgPrediction.add(new Tuple2<Integer, List<Tuple2<Integer, Double>>>(userid,temp2));
				}
				
				
				return weightedAvgPrediction;
			}
    		
    			
    	});
    	prediction.saveAsTextFile("Prediction");
    	 
    }
}
