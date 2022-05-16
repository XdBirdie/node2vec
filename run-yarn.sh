cmd=node2vec

spark-submit --class com.navercorp.Main \
    --master yarn --deploy-mode client \
    --num-executors 32 --executor-cores 12 \
    --driver-memory 32G --executor-memory 32G \
    --conf spark.driver.maxResultSize=32G \
    ./node2vec-0.1.2-SNAPSHOT.jar \
    --cmd $cmd --version one --directed true --indexed false --weighted false \
    --walkLength 20  --numWalks 5 --partitions 800 --iter 10 --window 20 \
    --logPath cit-Patents-One-timeline.log \
    --input graph/cit-Patents --output emb/cit-Patents-One.emb > cit-Patents-One.log
     
spark-submit --class com.navercorp.Main \
    --master yarn --deploy-mode client \
    --num-executors 32 --executor-cores 12 \
    --driver-memory 32G --executor-memory 32G \
    --conf spark.driver.maxResultSize=32G \
    ./node2vec-0.1.2-SNAPSHOT.jar \
    --cmd $cmd --version one --directed true --indexed false --weighted false \
    --walkLength 5  --numWalks 5 --partitions 800 --iter 10 --window 20 \
    --logPath soc-LiveJournal1-One-timeline.log \
    --input graph/soc-LiveJournal1 --output emb/soc-LiveJournal1-One.emb > soc-LiveJournal1-One.log


spark-submit --class com.navercorp.Main \
    --master yarn --deploy-mode client \
    --num-executors 32 --executor-cores 12 \
    --driver-memory 32G --executor-memory 32G \
    --conf spark.driver.maxResultSize=32G \
    ./node2vec-0.1.2-SNAPSHOT.jar \
    --cmd $cmd --version one --directed true --indexed false --weighted false \
    --walkLength 5  --numWalks 1 --partitions 800 --iter 10 --window 20  \
    --logPath uk-2002-One-timeline.log \
    --input graph/uk-2002 --output emb/uk-2002-One.emb > uk-2002-One.log
