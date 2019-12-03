#!/bin/bash

../meeseeks-box master.cfg &

../meeseeks-box headA.cfg &
../meeseeks-box headB.cfg &

../meeseeks-box nodeA1.cfg &
../meeseeks-box nodeA2.cfg &
../meeseeks-box nodeA3.cfg &

../meeseeks-box nodeB1.cfg &
../meeseeks-box nodeB2.cfg &
../meeseeks-box nodeB3.cfg &

while true; do sleep 1; done
