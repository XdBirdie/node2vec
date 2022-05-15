# nohup shell/test.sh > log/disk.log &

# Dataset=graph/BlogCatalog

# dataset=cit-Patents
# cmd=node2vec
# for version in one # join2 partition one broadcast baseline
# do
#   log_path=log/$dataset-$version-2.log
#   echo $log_path

#   spark-submit --class com.navercorp.Main \
#        --master yarn --deploy-mode client \
#        --num-executors 4 --executor-cores 96\
#        --driver-memory 50G --executor-memory 50G \
#        --conf spark.driver.maxResultSize=10G \
#        ./node2vec-0.1.2-SNAPSHOT.jar \
#        --cmd $cmd --version $version --directed false --indexed false --weighted false \
#        --walkLength 20  --numWalks 5 --degree 100 \
#        --partitions 800 \
#        --input graph/$dataset --output emb/Blog.emb > $log_path
# done

# dataset=cit-Patents
cmd=node2vec
for dataset in BlogCatalog # cit-Patents # BlogCatalog
do
  for version in one # join2 partition one broadcast baseline
  do
    log_path=log/$dataset-$version-1.log
    echo $log_path

    spark-submit --class com.navercorp.Main \
        --master spark://sepc724.se.cuhk.edu.hk:7077 \
        --driver-memory 50G \
        --executor-memory 50G \
        --executor-cores 4\
        --conf spark.driver.maxResultSize=10G\
        ./target/node2vec-0.1.2-SNAPSHOT.jar \
        --cmd $cmd --version $version --directed false --indexed false --weighted false \
        --walkLength 20  --numWalks 5 --degree 100 \
        --partitions 64  --iter 1 \
        --input graph/$dataset --output randompath/$dataset-rp > $log_path
  done
done

# TestDataset=graph/karate.edgelist

# spark-submit --class com.navercorp.Main \
#        --master spark://sepc724.se.cuhk.edu.hk:7077\
#        ./target/node2vec-0.1.2-SNAPSHOT.jar\
#        --cmd $cmd --version baseline --directed false --indexed false --weighted false\
#        --walkLength 20  --numWalks 5 --degree 100\
#        --input $Dataset --output emb/Blog.emb > log/blog_baseline1.log

# spark-submit --class com.navercorp.Main \
#        --master spark://sepc724.se.cuhk.edu.hk:7077\
#        ./target/node2vec-0.1.2-SNAPSHOT.jar\
#        --cmd $cmd --version broadcast --directed false --indexed false --weighted false\
#        --walkLength 20  --numWalks 5 --degree 100\
#        --input $Dataset --output emb/Blog.emb > log/blog_broadcast1.log

# spark-submit --class com.navercorp.Main \
#        --master spark://sepc724.se.cuhk.edu.hk:7077\
#        ./target/node2vec-0.1.2-SNAPSHOT.jar\
#        --cmd $cmd --version join2 --directed false --indexed false --weighted false\
#        --walkLength 20  --numWalks 5 --degree 100\
#        --input $Dataset --output emb/Blog.emb > log/blog_join2-1.log

# spark-submit --class com.navercorp.Main \
#        --master spark://sepc724.se.cuhk.edu.hk:7077\
#        ./target/node2vec-0.1.2-SNAPSHOT.jar\
#        --cmd $cmd --version partition --directed false --indexed false --weighted false\
#        --walkLength 20  --numWalks 5 --degree 100\
#        --input $Dataset --output emb/Blog.emb > log/blog_partition1.log

# spark-submit --class com.navercorp.Main \
#        --master spark://sepc724.se.cuhk.edu.hk:7077\
#        --driver-memory 20G \
#        ./target/node2vec-0.1.2-SNAPSHOT.jar\
#        --cmd $cmd --version one --directed false --indexed false --weighted false\
#        --walkLength 20  --numWalks 5 --degree 100\
#        --input $Dataset --output emb/Blog.emb > log/cit_one-1.log
