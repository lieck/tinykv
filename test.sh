#!/bin/bash
for ((i=1;i<=50;i++));
do
	echo "ROUND $i";
	make project2c > ./out/out-$i.txt;
done
