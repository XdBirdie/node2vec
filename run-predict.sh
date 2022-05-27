dataset=cit-Patents
cmd=linkPredict
spark-submit --class com.navercorp.Main \
    --master spark://sepc724.se.cuhk.edu.hk:7077 \
    --driver-memory 50G --executor-memory 50G --executor-cores 4\
    --conf spark.driver.maxResultSize=10G\
    ./target/node2vec-0.1.2-SNAPSHOT.jar \
    --cmd $cmd --version one --directed true --indexed true --weighted false \
    --partitions 64\
    --input $dataset --output emb/$dataset-linkPredict > log/$dataset-predict.log
