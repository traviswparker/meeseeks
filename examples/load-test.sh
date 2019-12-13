#!/bin/bash 

rm master.state
rm master.history

for c in 1 2 3 4 5 6 7 8 9; do

        ../meeseeks-box name=head${c} defaults.address=127.0.0.1 listen.port=137${c}0 nodes.node${c}1.port=137${c}1 nodes.node${c}2.port=137${c}2 nodes.node${c}3.port=137${c}3 nodes.node${c}4.port=137${c}4 nodes.node${c}5.port=137${c}5 nodes.node${c}6.port=137${c}6 nodes.node${c}7.port=137${c}7 nodes.node${c}8.port=137${c}8 nodes.node${c}9.port=137${c}9 &

    for n in 1 2 3 4 5 6 7 8 9; do

        ../meeseeks-box name=node${c}${n} listen.port=137${c}${n} pools.p1.slots=2 &

    done

done


../meeseeks-box name=master state.file=master.state state.checkpoint=60 state.history=master.history defaults.address=127.0.0.1 nodes.head1.port=13710 nodes.head2.port=13720 nodes.head3.port=13730 nodes.head4.port=13740 nodes.head5.port=13750 nodes.head6.port=13760 nodes.head7.port=13770 nodes.head8.port=13780 nodes.head9.port=13790 &

sleep 10

for j in {1..300}; do ../meeseeks-client id=job$j submit p1 sleep $j; done 

while true; do sleep 1; done
