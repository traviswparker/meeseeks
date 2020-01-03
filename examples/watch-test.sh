for d in 01 02 03 04 05 06 07 08 09
do
    mkdir -p $d
    rm -rf $d/*
    p=`pwd`
done

meeseeks-watch \
watch.job-01.template=job watch.job-01.path=`pwd`/01 \
watch.job-02.template=job watch.job-02.path=`pwd`/02 \
watch.job-03.template=job watch.job-03.path=`pwd`/03 \
watch.job-04.template=job watch.job-04.path=`pwd`/04 \
watch.job-05.template=job watch.job-05.path=`pwd`/05 \
watch.job-06.template=job watch.job-06.path=`pwd`/06 \
watch.job-07.template=job watch.job-07.path=`pwd`/07 \
watch.job-08.template=job watch.job-08.path=`pwd`/08 \
watch.job-09.template=job watch.job-09.path=`pwd`/09 \
watch.file-01.template=file watch.file-01.path=`pwd`/01 \
watch.file-02.template=file watch.file-02.path=`pwd`/02 \
watch.file-03.template=file watch.file-03.path=`pwd`/03 \
watch.file-04.template=file watch.file-04.path=`pwd`/04 \
watch.file-05.template=file watch.file-05.path=`pwd`/05 \
watch.file-06.template=file watch.file-06.path=`pwd`/06 \
watch.file-07.template=file watch.file-07.path=`pwd`/07 \
watch.file-08.template=file watch.file-08.path=`pwd`/08 \
watch.file-09.template=file watch.file-09.path=`pwd`/09 \
watch.fileset-01.template=fileset watch.fileset-01.path=`pwd`/01 \
watch.fileset-02.template=fileset watch.fileset-02.path=`pwd`/02 \
watch.fileset-03.template=fileset watch.fileset-03.path=`pwd`/03 \
watch.fileset-04.template=fileset watch.fileset-04.path=`pwd`/04 \
watch.fileset-05.template=fileset watch.fileset-05.path=`pwd`/05 \
watch.fileset-06.template=fileset watch.fileset-06.path=`pwd`/06 \
watch.fileset-07.template=fileset watch.fileset-07.path=`pwd`/07 \
watch.fileset-08.template=fileset watch.fileset-08.path=`pwd`/08 \
watch.fileset-09.template=fileset watch.fileset-09.path=`pwd`/09 \
watch.fileupdate-01.template=fileupdate watch.fileupdate-01.path=`pwd`/01 \
watch.fileupdate-02.template=fileupdate watch.fileupdate-02.path=`pwd`/02 \
watch.fileupdate-03.template=fileupdate watch.fileupdate-03.path=`pwd`/03 \
watch.fileupdate-04.template=fileupdate watch.fileupdate-04.path=`pwd`/04 \
watch.fileupdate-05.template=fileupdate watch.fileupdate-05.path=`pwd`/05 \
watch.fileupdate-06.template=fileupdate watch.fileupdate-06.path=`pwd`/06 \
watch.fileupdate-07.template=fileupdate watch.fileupdate-07.path=`pwd`/07 \
watch.fileupdate-08.template=fileupdate watch.fileupdate-08.path=`pwd`/08 \
watch.fileupdate-09.template=fileupdate watch.fileupdate-09.path=`pwd`/09 \
watch.multi-01.template=multi \
watch.cfg