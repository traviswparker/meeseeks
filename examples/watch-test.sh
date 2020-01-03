for d in 01 02 03 04 05 06 07 08 09
do
    mkdir -p $d
    rm -rf $d/*
    p=`pwd`
done


start() {
    meeseeks-watch name=$1 watch.$1-01.template=$1 watch.$1-01.path=`pwd`/01 watch.$1-02.template=$1 watch.$1-02.path=`pwd`/02 watch.$1-03.template=$1 watch.$1-03.path=`pwd`/03 watch.$1-04.template=$1 watch.$1-04.path=`pwd`/04 watch.$1-05.template=$1 watch.$1-05.path=`pwd`/05 watch.$1-06.template=$1 watch.$1-06.path=`pwd`/06 watch.$1-07.template=$1 watch.$1-07.path=`pwd`/07 watch.$1-08.template=$1 watch.$1-08.path=`pwd`/08 watch.$1-09.template=$1 watch.$1-09.path=`pwd`/09 watch.cfg &
}

start 'job'
start 'file'
start 'fileset'
start 'fileupdate'
meeseeks-watch name=multi watch.multi-01.template=multi watch.cfg &

while true; do sleep 1; done
