#!/bin/bash

meeseeks-box user=nobody master.cfg &

meeseeks-box user=nobody headA1.cfg &
meeseeks-box user=nobody headB1.cfg &
meeseeks-box user=nobody headA2.cfg &
meeseeks-box user=nobody headB2.cfg &

meeseeks-box user=nobody nodeA1.cfg &
meeseeks-box user=nobody nodeA2.cfg &
meeseeks-box user=nobody nodeA3.cfg &

meeseeks-box user=nobody nodeB1.cfg &
meeseeks-box user=nobody nodeB2.cfg &
meeseeks-box user=nobody nodeB3.cfg &

while true; do sleep 1; done
