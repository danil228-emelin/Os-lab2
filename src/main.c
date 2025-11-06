#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "ram_bench.h"


#define DEFAULT_ITERATIONS 1000
#define DEFAULT_DATA_SIZE 1000

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file_name> [iterations] [data_size]\n", argv[0]);
        fprintf(stderr, "  file_name   - output file for results\n");
        fprintf(stderr, "  iterations  - number of EMA-JOIN iterations (default: %d)\n", DEFAULT_ITERATIONS);
        fprintf(stderr, "  data_size   - size of data arrays (default: %d)\n", DEFAULT_DATA_SIZE);
        return EXIT_FAILURE;
    }

    const char *file_name = argv[1];
    int iterations = (argc > 2) ? atoi(argv[2]) : DEFAULT_ITERATIONS;
    int data_size = (argc > 3) ? atoi(argv[3]) : DEFAULT_DATA_SIZE;

    printf("Starting EMA-JOIN benchmark...\n");
    printf("Output file: %s\n", file_name);
    printf("Iterations: %d\n", iterations);
    printf("Data size: %d elements\n", data_size);

    // Создаем или открываем файл для результатов
    int fd = open(file_name, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        perror("Error opening results file");
        return EXIT_FAILURE;
    }

    clock_t start_time = clock();

    // Запускаем EMA-JOIN вычисления
    double final_result = perform_ema_join_calculation(iterations, data_size, file_name);

    clock_t end_time = clock();
    double duration = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    printf("EMA-JOIN calculations completed!\n");
    printf("Final result: %f\n", final_result);
    printf("Execution time: %f seconds\n", duration);

    // Записываем результаты в файл
    char result_buffer[256];
    int len = snprintf(result_buffer, sizeof(result_buffer),
                      "Iterations: %d, Data size: %d, Result: %f, Time: %f sec\n",
                      iterations, data_size, final_result, duration);
    write(fd, result_buffer, len);

    close(fd);
    return EXIT_SUCCESS;
}