gcc -o analyze analyze.c heartyhty_functions.c ../third_party/cJSON/cJSON.c
# valgrind --leak-check=yes ./analyze
./analyze
