spark-submit --class com.navercorp.Main --master spark://sepc724.se.cuhk.edu.hk:7077\
       ./target/node2vec-0.1.2-SNAPSHOT.jar\
       --cmd randomwalk --directed false --indexed false --weighted false\
       --input  ./graph/karate.edgelist --output ./result/out
