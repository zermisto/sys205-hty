gcc -o analyze analyze.c ../third_party/cJSON/cJSON.c
# valgrind --leak-check=yes ./analyze
./analyze