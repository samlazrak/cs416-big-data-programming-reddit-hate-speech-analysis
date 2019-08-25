#!/usr/bin/env python

import os

if __name__ == '__main__':

    words = []
             
    count = 0

    # Open input and output files.
    with open("vocabulary.txt", 'wb') as outFile, open("vocabIn.txt", 'rb') as inFile, open("words_removed.txt", 'rb') as rmFile:
        words_removed = []
        for line in rmFile:
            words_removed.append(line.strip())
            
        inFile.readline()
        for line in inFile:
            t = line.split(';')
            if t[3] == 'eng' and not t[0] in words_removed and len(t[0].split()) == 1:
                words.append(t[0])

        words.sort()
        
        line = ""
        
        for i in range(len(words)):
            line += words[i]
            line += ' '
            
        outFile.write(line)
            
    print("Vocabulary size: %d" % count)
