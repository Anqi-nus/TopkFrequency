# TopkFrequency
CS4225 Hadoop MapReduce Assignment. 

## Problem Definition:  
Given TWO textual files, for each common word between the two files, find the bigger number of times that it appears between the two files, we call this number as the frequency of the word.  
- Example: if the word “John” appears 5 times in the 1st file and 3 times in the 2nd file, the frequency of “John” is 5.

## Requirements
- Split the input text with “(space)\t\n\r\f”. Any other tokens like “,.:`” will be regarded as a part of the words. 
- Remove stop-words as given in Stopwords.txt, such as “a”, “the”, “that”, “of”, ... (case sensitive)
- Sort the common words in descending order of the frequency.
- If two words have the same frequency, sort them based on the descending lexicographic order [1]. For example, if the word ‘a’, ‘ab’ and ‘A’ have the same frequency, you should sort them as ‘ab’, ‘a’, ‘A’.
- In general, words with different case or different non- whitespace punctuation are considered different words
