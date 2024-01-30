#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdint.h>
#include <time.h>

#define ASSERT_NOT_EQUAL(ret, val, where)                                                                               \
    do {                                                                                                     \
        if ((ret) == (val)) {                                                                                \
          long _ret = (long) ret, _val = (long) val;\
          printf("Unexpected return at %s: %ld == %ld\n",  where, _ret, _val);\
          exit(1);\
        }                                                                                                    \
    } while (0)

#define ASSERT_EQUAL(_x, _val, where)                                                                              \
    do {                                                                                                     \
        long __x = (long)_x, __val = (long)_val;                                                             \
        if ((__x) != (__val)) {                                                                              \
            printf("Unexpected return at %s: %ld != %ld\n", where, __x, __val);\
                      exit(1);\
        }                                                                                                    \
    } while (0)

#define ASSERT_TRUE(b) \
    do {\
        if (!(b)) {\
            printf("Assertion failed");\
            exit(1);\
        }\
    } while (0)

#define RAND_INT(min, max) (rand() % (max - min + 1) + min)

#define MAX_LEN 100
#define LOWERCASE_A_ASCII 97
#define LOWERCASE_Z_ASCII 122
#define FAIL -1

/* Number of bytes used to store size of each string */
#define NUM_SIZE_BYTES 4

/* Serialize an array of N pointers to variable-length strings to a byte array. */
char* array_to_bytes(const char** arr_in, size_t num_strings, size_t *lengths);

/*  Deserialize an array of bytes to an array of pointers to variable-length strings. */
char **bytes_to_array(char *bytes_in, size_t num_strings);

/* Generate num_strings strings, where the i-th string has size lengths[i]. */
char **generate_random_strings(size_t num_strings, size_t *lengths);

/* Show strings in the array for debugging */
void display_strings(char **strings, size_t num_strings);

/* Compute elapsed time in seconds from two timespecs */
double get_elapsed(struct timespec start, struct timespec end);

/* Time serialization and deserialization of a buffer of pointers to variable length strings

Example output:

$ ./bytesToVlen 10000
Total amount of data = 507511 bytes
array_to_bytes took 0.000248 seconds for 10000 elements (2043037719 bytes/sec)
array_to_bytes took 0.000353 seconds for 10000 elements (1438368316 bytes/sec)
Serialization and deserialization values are correct
Benchmark complete
*/
int main(int argc, char *argv[]) {
    size_t num_strings = 0;
    size_t total_size = 0;

    size_t *str_lengths = NULL;
    char **strings_arr = NULL;
    char **deserialized_strings_arr = NULL;

    char *serialized_bytes = NULL;
    char *bytes_ptr = NULL;

    double elapsed = 0.0;
    struct timespec start;
    struct timespec end;

    /* Parse commmand line arguments */
    if (argc != 2) {
        printf("usage: ./vlenBenchmark count\n");
        exit(1);
    }

    if ((num_strings = strtol(argv[1], NULL, 10)) == 0) {
        printf("count must be greater than 0\n");
        exit(1);
    }

    /* Allocate memory for lengths */
    str_lengths = calloc(num_strings, sizeof(size_t));

    /* Generate strings to serialize and deserialize */
    strings_arr = generate_random_strings(num_strings, str_lengths);
    
    for (size_t i = 0; i < num_strings; i++) {
        total_size += str_lengths[i];
    }

    printf("Total amount of data = %zu bytes\n", total_size);

    /* Time serialization to bytes */
    ASSERT_NOT_EQUAL(clock_gettime(CLOCK_MONOTONIC, &start), FAIL, "clock_gettime");

    serialized_bytes = array_to_bytes((const char **) strings_arr, num_strings, str_lengths);

    ASSERT_NOT_EQUAL(clock_gettime(CLOCK_MONOTONIC, &end), FAIL, "clock_gettime");

    elapsed = get_elapsed(start, end);

    printf("array_to_bytes took %lf seconds for %zu elements (%zu bytes/sec)\n", elapsed, num_strings, (size_t) ((double) total_size / elapsed));

    /* Time deserialization back to array of pointers */
    ASSERT_NOT_EQUAL(clock_gettime(CLOCK_MONOTONIC, &start), FAIL, "clock_gettime");

    deserialized_strings_arr = bytes_to_array(serialized_bytes, num_strings);

    ASSERT_NOT_EQUAL(clock_gettime(CLOCK_MONOTONIC, &end), FAIL, "clock_gettime");
    
    elapsed = get_elapsed(start, end);

    printf("bytes_to_array took %lf seconds for %zu elements (%zu bytes/sec)\n", elapsed, num_strings, (size_t) ((double) total_size / elapsed));

    /* Check Correctness */
    for (size_t i = 0; i < num_strings; i++) {
        ASSERT_TRUE(!strcmp(strings_arr[i], deserialized_strings_arr[i]));
        ASSERT_EQUAL(str_lengths[i], strlen(deserialized_strings_arr[i]) + 1, "string size check");
    }

    printf("Serialization and deserialization values are correct\n");

    /* Clean up */
    if (strings_arr) {
        for (size_t i = 0; i < num_strings; i++) {
            if (strings_arr[i]) {
                free(strings_arr[i]);
            }
        }

        free(strings_arr);
    }

    if (deserialized_strings_arr) {
        for (size_t i = 0; i < num_strings; i++) {
            if (deserialized_strings_arr[i]) {
                free(deserialized_strings_arr[i]);
            }
        }

        free(deserialized_strings_arr);
    }

    if (serialized_bytes) {
        free(serialized_bytes);
    }

    if (str_lengths) {
        free(str_lengths);
    }

    printf("Benchmark complete\n");
}

/* Serialize an array of N pointers to variable-length strings to a byte array.
   Each element is NUM_SIZE_BYTES bytes to describe the size of the string, then the string itself.
*/
char* array_to_bytes(const char** arr_in, size_t num_strings, size_t *lengths) {
    size_t total_size = 0;
    char *bytes_out = NULL;
    char *bytes_ptr = NULL;

    /* Determine size and allocate memory */
    total_size += NUM_SIZE_BYTES * num_strings;
    for (size_t i = 0; i < num_strings; i++) {
        total_size += lengths[i];
    }

    bytes_out = malloc(total_size);
    bytes_ptr = bytes_out;

    /* Serialize each element */
    for (size_t i = 0; i < num_strings; i++) {
        /* Write size */
        /* Cast size_t to uint32_t to only copy lower bits */
        memcpy(bytes_ptr, (uint32_t*) &lengths[i], NUM_SIZE_BYTES);
        bytes_ptr += NUM_SIZE_BYTES;

        /* Write string */
        memcpy(bytes_ptr, arr_in[i], lengths[i]);
        bytes_ptr += lengths[i];
    }

    return bytes_out;
}

/*  Deserialize an array of bytes to an array of pointers to variable-length strings. 
    The strings are serialized as described in array_to_bytes
*/
char **bytes_to_array(char *bytes_in, size_t num_strings) {
    char *bytes_ptr = bytes_in;
    char **strings_out = NULL;

    strings_out = calloc(num_strings, sizeof(char*));

    for (size_t i = 0; i < num_strings; i++) {
        char curr_string[MAX_LEN];
        size_t curr_size = 0;

        /* Read size of string */
        curr_size = (size_t) *((uint32_t*) bytes_ptr);
        bytes_ptr += NUM_SIZE_BYTES;

        strings_out[i] = malloc(curr_size);
        /* Read string value */
        memcpy(strings_out[i], bytes_ptr, curr_size);
        bytes_ptr += curr_size;
    }

    return strings_out;
}

/* Generate num_strings strings, where the i-th string has size lengths[i]. */
char **generate_random_strings(size_t num_strings, size_t *lengths) {
    /* Generate num_strings lengths between 1 and max_len inclusive */
    ASSERT_NOT_EQUAL(lengths, NULL, "lengths arg to generate random strings");

    char **strings_out = NULL;

    for (size_t i = 0; i < num_strings; i++) {
        lengths[i] = RAND_INT(1, MAX_LEN);
    }

    /* Allocate memory for strings */
    strings_out = calloc(num_strings, sizeof(char*));

    for (size_t i = 0; i < num_strings; i++) {
        strings_out[i] = malloc(lengths[i]);
    }

    /* Populate string buffers */
    for (size_t i = 0; i < num_strings; i++) {
        for (size_t c_idx = 0; c_idx < lengths[i] - 1; c_idx++) {
            char random_char = (char) RAND_INT(LOWERCASE_A_ASCII, LOWERCASE_Z_ASCII);
            strings_out[i][c_idx] = random_char;
        }

        strings_out[i][lengths[i] - 1] = '\0';
    }

    return strings_out;
}

/* Show strings in the array for debugging */
void display_strings(char **strings, size_t num_strings) {
    for (size_t i = 0; i < num_strings; i++) {
        printf("String #%zu: %s\n", i, strings[i]);
    }
}

/* Compute elapsed time in seconds from two timespecs */
double get_elapsed(struct timespec start, struct timespec end) {
    double out = 0.0;

    out += (end.tv_sec - start.tv_sec);
    out += (end.tv_nsec - start.tv_nsec) / pow(10.0, 9);

    return out;
}