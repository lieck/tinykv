#!/bin/bash
for ((i=1;i<=150;i++));
do
	echo "ROUND $i";
	make project2b > ./out/out-$i.txt;
done
