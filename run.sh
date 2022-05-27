# hdfs dfs -rm -r emb/*
# cmd=randomwalk
# for dataset in cit-Patents  # BlogCatalog # cit-Patents # soc-LiveJournal1 # BlogCatalog # cit-Patents # BlogCatalog
# do
#   for version in baseline # one # join2 partition one broadcast baseline
#   do
#     log_path=log/$dataset-$version-3.log
#     echo $log_path

#     spark-submit --class com.navercorp.Main \
#         --master spark://sepc724.se.cuhk.edu.hk:7077 \
#         --driver-memory 50G --executor-memory 50G --executor-cores 4\
#         --conf spark.driver.maxResultSize=10G\
#         ./target/node2vec-0.1.2-SNAPSHOT.jar \
#         --cmd $cmd --version $version --directed false --indexed false --weighted false \
#         --walkLength 20  --numWalks 5 --degree 100 \
#         --partitions 64  --iter 10 \
#         --input graph/$dataset --output emb/$dataset-$version \
#         --logPath ./log.out > $log_path
#   done
# done
cmd=node2vec
dataset=cit-Patents
spark-submit --class com.navercorp.Main \
    --master spark://sepc724.se.cuhk.edu.hk:7077 \
    --driver-memory 50G --executor-memory 50G --executor-cores 4\
    --conf spark.driver.maxResultSize=10G\
    ./target/node2vec-0.1.2-SNAPSHOT.jar \
    --cmd $cmd --version one --directed true --indexed true --weighted false \
    --walkLength 20 --numWalks 5 --partitions 64 --iter 10 --window 20 --dim 32\
    --logPath $dataset-One-timeline.log \
    --input graph/$dataset --output emb/$dataset-One > log/$dataset-One.log