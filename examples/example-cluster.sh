#!/bin/bash

../meeseeks-box master.cfg &

../meeseeks-box headA1.cfg &
../meeseeks-box headB1.cfg &
../meeseeks-box headA2.cfg &
../meeseeks-box headB2.cfg &

../meeseeks-box nodeA1.cfg &
../meeseeks-box nodeA2.cfg &
../meeseeks-box nodeA3.cfg &

../meeseeks-box nodeB1.cfg &
../meeseeks-box nodeB2.cfg &
../meeseeks-box nodeB3.cfg &

while true; do sleep 1; done
