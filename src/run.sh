gcc -o csv_to_hty csv_to_hty.c ../third_party/cJSON/cJSON.c
./csv_to_hty
# valgrind --leak-check=yes ./csv_to_hty