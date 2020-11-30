for d in 01 02 03 04 05 06 07 08 09
do
    mkdir -p $d
    rm -rf $d/*
    p=`pwd`
done

test() {
    meeseeks-watch defaults.template=$1 \
        watch.01.path=`pwd`/01 \
        watch.02.path=`pwd`/02 \
        watch.03.path=`pwd`/03 \
        watch.04.path=`pwd`/04 \
        watch.05.path=`pwd`/05 \
        watch.06.path=`pwd`/06 \
        watch.07.path=`pwd`/07 \
        watch.08.path=`pwd`/08 \
        watch.09.path=`pwd`/09 \
    watch.cfg 
}

test job,file,fileset,fileupdate &
meeseeks-watch defaults.template=multi watch.01= watch.cfg