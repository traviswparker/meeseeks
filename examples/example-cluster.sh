#!/bin/bash

cfg='user=nobody'

meeseeks-box $cfg master.cfg &

meeseeks-box $cfg headA1.cfg &
meeseeks-box $cfg headB1.cfg &
meeseeks-box $cfg headA2.cfg &
meeseeks-box $cfg headB2.cfg &

meeseeks-box $cfg nodeA1.cfg &
meeseeks-box $cfg nodeA2.cfg &
meeseeks-box $cfg nodeA3.cfg &

meeseeks-box $cfg nodeB1.cfg &
meeseeks-box $cfg nodeB2.cfg &
meeseeks-box $cfg nodeB3.cfg &

while true; do sleep 1; done
