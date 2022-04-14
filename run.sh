# nohup shell/test.sh > log/disk.log &

spark-submit --class com.navercorp.Main \
       --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd node2vec --version baseline --directed false --indexed false --weighted false\
       --walkLength 20  --numWalks 5 --degree 100\
       --input graph/BlogCatalog --output emb/Blog.emb > log/blog_baseline1.log

spark-submit --class com.navercorp.Main \
       --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd node2vec --version broadcast --directed false --indexed false --weighted false\
       --walkLength 20  --numWalks 5 --degree 100\
       --input graph/BlogCatalog --output emb/Blog.emb > log/blog_broadcast1.log

spark-submit --class com.navercorp.Main \
       --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd node2vec --version join2 --directed false --indexed false --weighted false\
       --walkLength 20  --numWalks 5 --degree 100\
       --input graph/BlogCatalog --output emb/Blog.emb > log/blog_join2-1.log

spark-submit --class com.navercorp.Main \
       --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd node2vec --version partition --directed false --indexed false --weighted false\
       --walkLength 20  --numWalks 5 --degree 100\
       --input graph/BlogCatalog --output emb/Blog.emb > log/blog_partition1.log
