-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.
-- Input: (year,news)
-- Input: (word,label)
rec = LOAD 'abcnews.csv' USING PigStorage(',') AS (year:chararray,news:chararray);
mix = LOAD 'positivesnegatives.csv' USING PigStorage(',') AS (word:chararray,label:chararray);
-- Select the news 
-- newsHeadlines:{news:chararray}
newsHeadlines = FOREACH rec GENERATE news;
-- tokenize the line in newsHeadlines.news
-- tokenizedHeadline: {news: chararray,word: {tuple_of_tokens: (token: chararray)}}
tokenizedHeadline = FOREACH newsHeadlines GENERATE news, TOKENIZE(news) as word;
-- flatten the word
-- tokenizedFlatten: {news: chararray,s: chararray}
tokenizedFlatten = FOREACH tokenizedHeadline GENERATE news, FLATTEN(word) as s;
-- group the headline by news
-- grpNews: {group: chararray,tokenizedFlatten: {(news: chararray,s: chararray)}}
grpNews = GROUP tokenizedFlatten BY news;
-- Filter negative filters from sentiments data
-- negFilter: {word: chararray,label: chararray}
negFilter = FILTER mix by label=='negative';
-- select the words
-- grpNegFil: {neg: chararray}
grpNegFil = FOREACH negFilter GENERATE word as neg;
-- Join the negwords and the headlines
-- joinneg: {tokenizedFlatten::news: chararray,tokenizedFlatten::s: chararray,grpNegFil::neg: chararray}
joinneg = JOIN tokenizedFlatten by s, grpNegFil by neg;
-- select the news and s
-- selectnewsAndWord: {tokenizedFlatten::news: chararray,tokenizedFlatten::s: chararray}
selectnewsAndWord = FOREACH joinneg GENERATE news,s;
-- group selectnewsAndWord by news
-- groupnegative: {group: chararray,selectnewsAndWord: {(tokenizedFlatten::news: chararray,tokenizedFlatten::s: chararray)}}
groupnegative = GROUP selectnewsAndWord by news;
-- count the negative words
-- wordnegative: {g: chararray,negativecount: long}
wordnegative  = FOREACH groupnegative GENERATE group as g, COUNT(selectnewsAndWord.s) as negativecount;
-- Now do the same thing for positive and neutral
--POSTIVE
posFilter = FILTER mix by label=='positive';
grpPosFil = FOREACH posFilter GENERATE word as pos;
joinpos = JOIN tokenizedFlatten by s, grpPosFil by pos;
selectnewsAndWordPos = FOREACH joinpos GENERATE news, s;
grouppositive = GROUP selectnewsAndWordPos by news;
wordpositive = FOREACH grouppositive GENERATE group as g, COUNT(selectnewsAndWordPos.s) as positivecount;
-- NEUTRAL
neuFilter = FILTER mix by label=='neutral';
grpNeuFil = FOREACH neuFilter GENERATE word as neu;
joinneu = JOIN tokenizedFlatten by s, grpNeuFil by neu;
selectnewsAndWordNeu = FOREACH joinneu GENERATE news,s;
groupneutral = GROUP selectnewsAndWordNeu by news;
wordneutral = FOREACH groupneutral GENERATE group as g, COUNT(selectnewsAndWordNeu.s) as neutralcount;

-- Now combine all of the counts into single tuple
--result = UNION wordnegative, wordpositive;
--result = UNION result, wordneutral;
-- Store the result 
--Having space issues in virtual machine
STORE wordnegative INTO 'result' using PigStorage('|');




