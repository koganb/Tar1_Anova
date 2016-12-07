Tar1_Anova 
Program output:

small.txt
- K : 3
- N : 16
- F : 11.284722222222221





big.txt
- K : 4
- N : 1000000
- F : 636254.1369107931




Commands:
- docker exec -u 0 -it sandbox bash
- spark-submit --class org.bgu.onewayanova.OneWayAnova --master local  OneWayAnova-1.0-all.jar /tmp/small.txt
