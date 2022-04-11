# nohup shell/test.sh > log/mem16-inv.log &

spark-submit --class com.navercorp.Main \
       --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd node2vec --directed false --indexed false --weighted false\
       --walkLength 20  --numWalks 5\
       --input graph/BlogCatalog --output emb/Blog.emb > log/node2vec16-inv.log
