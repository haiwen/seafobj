#!/bin/bash

printf "\033[1;32m[ rm   ]\033[m deleting the following old files\n"

ls -lh cscope.*

rm cscope.*

printf "\033[1;32m[ find ]\033[m write source file list to cscope.files\n"

find . -name '*.py' > cscope.files

printf "\033[1;32m[  wc  ]\033[m \033[0;40;32m`wc -l cscope.files | awk '{print $1;}'`\033[m files found\n"

printf "\033[1;32m[cscope]\033[m now index source files\n"

pycscope -i cscope.files

du -h cscope.*

printf "\033[1;32m[cscope]\033[m done\n"

