for d in 01 02 03 04 05 06 07 08 09
do
    mkdir -p $d
    rm -rf $d/*
    p=`pwd`
done

t='jobtest'

meeseeks-watch logging.level=10 watch.${t}-01.template=$t watch.${t}-01.path=`pwd`/01 watch.${t}-02.template=$t watch.${t}-02.path=`pwd`/02 watch.${t}-03.template=$t watch.${t}-03.path=`pwd`/03 watch.${t}-04.template=$t watch.${t}-04.path=`pwd`/04 watch.${t}-05.template=$t watch.${t}-05.path=`pwd`/05 watch.${t}-06.template=$t watch.${t}-06.path=`pwd`/06 watch.${t}-07.template=$t watch.${t}-07.path=`pwd`/07 watch.${t}-08.template=$t watch.${t}-08.path=`pwd`/08 watch.${t}-09.template=$t watch.${t}-09.path=`pwd`/09 watch.cfg &

t='filetest'

meeseeks-watch logging.level=10 watch.${t}-01.template=$t watch.${t}-01.path=`pwd`/01 watch.${t}-02.template=$t watch.${t}-02.path=`pwd`/02 watch.${t}-03.template=$t watch.${t}-03.path=`pwd`/03 watch.${t}-04.template=$t watch.${t}-04.path=`pwd`/04 watch.${t}-05.template=$t watch.${t}-05.path=`pwd`/05 watch.${t}-06.template=$t watch.${t}-06.path=`pwd`/06 watch.${t}-07.template=$t watch.${t}-07.path=`pwd`/07 watch.${t}-08.template=$t watch.${t}-08.path=`pwd`/08 watch.${t}-09.template=$t watch.${t}-09.path=`pwd`/09 watch.cfg &

t='filesettest'

meeseeks-watch logging.level=10 watch.${t}-01.template=$t watch.${t}-01.path=`pwd`/01 watch.${t}-02.template=$t watch.${t}-02.path=`pwd`/02 watch.${t}-03.template=$t watch.${t}-03.path=`pwd`/03 watch.${t}-04.template=$t watch.${t}-04.path=`pwd`/04 watch.${t}-05.template=$t watch.${t}-05.path=`pwd`/05 watch.${t}-06.template=$t watch.${t}-06.path=`pwd`/06 watch.${t}-07.template=$t watch.${t}-07.path=`pwd`/07 watch.${t}-08.template=$t watch.${t}-08.path=`pwd`/08 watch.${t}-09.template=$t watch.${t}-09.path=`pwd`/09 watch.cfg &

while true; do sleep 1; done
