Item-item collaborative filtering  is a form of collaborative filtering based on the similarity between items calculated using people's ratings of those items.

This project is implemented using Movie Lens Dataset. http://grouplens.org/datasets/movielens/

Implementation:

1) Item Similarity Computation:
        First, we need to find similarity between two items(movies here). Ignore the record if user is not rated for any of  those 2 items. I computed cosine similarity measure. I would recommend you to try with different similarity measure like Jaccard     similarity, Adjusted Cosine Similarity,Correlation-based Similarity
        
2) Prediction Computation:
   For each item, that is not rated by user, we need to predict rating. I computed using Weighted Sum. I would recommend you to try with other measures like Regression techniques.
        
Note: Handling Missing values 

Method 1: Since we had rating values in the range 1 to 5. I assumed missing value as 0 for movie that is not rated by user.

Method 2: If rating value is in the range from -5 to 5. Then assigning 0 to the missing value will give wrong results when we find similarity measure and prediction. To overcome this situation, if the rating value is integer, then we can have rating values from 1 to 10 instead of -5 to 5 and then assigning 0 to the missing value ratings. But, if the rating values are floats, then specify infinity value as flag for missing value and identify missing value while computing cosine similarity. 

I would recommend to go the following link for more information regarding Item Based Collaborative Filtering. http://files.grouplens.org/papers/www10_sarwar.pdf

Spark implementation Summary: 

Stage1 :
    Load the data into RDD. Find list of users,movies using that RDD. Also, create JavaPairRDD ratings, where key is user id. Value is Tuple2<Movie,Rating>
    
Stage2 :
    Create Utility Matrix RDD from ratings RDD. For each each user, we will aggregate all the information from ratings RDD to create RDD of the form <userid><Rating1,Rating2,......>. Missing values are handled using above methods. 
    
Stage3: 
    Create CosineMatrix RDD from Utility Matrix RDD. In this RDD,  we compute cosine similarity measure between every pair of items. So, this RDD is of the form item X item matrix. Each row corresponds to cosine similarity distance measure between one movie to every other movie.
    
Stage4:
   Created Prediction RDD using Cosine Matrix and Utility Matrix. First, we need to fill all the missing values, that is predicting rating of item(movie), that is not rated by user. Then from the list of these <movie,rating>  values, return the top N( here I have taken N as 10) recommendations. So we return these Top N movies, that were not rated by user based on predicted ratings.
   
I would like to thank my professor Dr.Mohamed Sarwat for giving me this opportunity to work with him. As part of this project, I reported run time of jobs, intermediate RDDs created, CPU Utilization, Memory Usage of 3 worker nodes.

In case if you have any queries regarding this project or if you have any suggestions to improve this project, please reach out to me at kranthisai.d@gmail.com. I will be happy in assisting you or I will appreciate your contribution to this project. Thank you. Have A Nice Day....





