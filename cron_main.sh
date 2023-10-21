d=$1
while [ "$d" != $2 ]; do
  echo $d
  ./main.sh $d
  d=$(date -I -d "$d + 1 day")

done

