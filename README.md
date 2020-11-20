# Web-Scrapping
Web Scrapping 
Implementing Content Similarity on Web Scrapped Data

Here I have used BeautifulSoup for scrapping the data from the given Url
After scrapping on different Url's saved the each text data on 3 different txt files. 

# Content Similarity
Similarity between two sentences can be achieved by calculating the Cosine Similairty or Euclidian Distance between each word vector
Here I have used Cosine Similarity to calculate the similarity between the words
Initially I tought of using 2 different files for comparing each and every sentence from 2 files.
But given the time constraint, the similarity is done only using one text file
 
cosine similarity is the dot/scalar product of two vectors divided by the product of their Euclidian norms
Process to do the Similiarity
1. Raw text data is preprocessed by stemming, lemmatisation, tokenization
2. Term frequency of each word is calculated and a dictionary of the word frequency is created
3. The numbers are used to create a vector for each document where each component in the vector stands for the term frequency in that document. Let n be the number of documents and m be the number of unique terms. Then we have an n by m tf matrix.
4. The core of the rest is to obtain a “term frequency-inverse document frequency” (tf-idf) matrix. Inverse document frequency is an adjustment to term frequency. 
5. tf-idf scales up the importance of rarer terms and scales down the importance of more frequent terms relative to the whole corpus.
                   
                   tf-idf = tf * idf
                   idf(t) = log(n+1/df(d,t)+1)+1
                   where n is the total number of documents and df(d, t) is the number of documents in which term t appears
                   as df(d, t) gets smaller, idf(t) gets larger
6. The calculated tf-idf is normalized by the Cosine Similarity formula so that each row vector has a length of 1
7. The normalized tf-idf matrix should be in the shape of n by m. A cosine similarity matrix (n by n) can be obtained by multiplying the if-idf matrix by its transpose (m by n).

I just did similarity to just one document, due to time constraint, full model development is not done
