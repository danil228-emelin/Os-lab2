#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <dlfcn.h>  // для динамической загрузки библиотеки

#include "ram_bench.h"
#include "ccache.h"  // заголовочный файл кэширующей библиотеки

#define DEFAULT_ITERATIONS 1000
#define DEFAULT_DATA_SIZE 1000

// Объявления функций из кэширующей библиотеки
typedef int (*lab2_open_t)(const char*);
typedef int (*lab2_close_t)(int);
typedef ssize_t (*lab2_read_t)(int, void*, size_t);
typedef ssize_t (*lab2_write_t)(int, const void*, size_t);
typedef off_t (*lab2_lseek_t)(int, off_t, int);
typedef int (*lab2_fsync_t)(int);

// Указатели на функции
static lab2_open_t lab2_open_ptr = NULL;
static lab2_close_t lab2_close_ptr = NULL;
static lab2_read_t lab2_read_ptr = NULL;
static lab2_write_t lab2_write_ptr = NULL;
static lab2_lseek_t lab2_lseek_ptr = NULL;
static lab2_fsync_t lab2_fsync_ptr = NULL;

// Загрузка библиотеки
int load_cache_library() {
    void* handle = dlopen("libccache_lib.so", RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "Error loading library: %s\n", dlerror());
        return -1;
    }

    // Загружаем функции
    lab2_open_ptr = (lab2_open_t)dlsym(handle, "lab2_open");
    lab2_close_ptr = (lab2_close_t)dlsym(handle, "lab2_close");
    lab2_read_ptr = (lab2_read_t)dlsym(handle, "lab2_read");
    lab2_write_ptr = (lab2_write_t)dlsym(handle, "lab2_write");
    lab2_lseek_ptr = (lab2_lseek_t)dlsym(handle, "lab2_lseek");
    lab2_fsync_ptr = (lab2_fsync_t)dlsym(handle, "lab2_fsync");

    // Проверяем что все функции загружены
    if (!lab2_open_ptr || !lab2_close_ptr || !lab2_read_ptr || 
        !lab2_write_ptr || !lab2_lseek_ptr || !lab2_fsync_ptr) {
        fprintf(stderr, "Error loading functions: %s\n", dlerror());
        dlclose(handle);
        return -1;
    }

    return 0;
}

// Функция для записи результатов с использованием кэша
void write_with_cache(int cache_fd, const char* buffer, size_t length) {
    size_t written = 0;
    while (written < length) {
        ssize_t result = lab2_write_ptr(cache_fd, buffer + written, length - written);
        if (result < 0) {
            perror("Error writing with cache");
            break;
        }
        written += result;
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file_name> [iterations] [data_size]\n", argv[0]);
        fprintf(stderr, "  file_name   - output file for results\n");
        fprintf(stderr, "  iterations  - number of EMA-JOIN iterations (default: %d)\n", DEFAULT_ITERATIONS);
        fprintf(stderr, "  data_size   - size of data arrays (default: %d)\n", DEFAULT_DATA_SIZE);
        return EXIT_FAILURE;
    }

    // Загружаем кэширующую библиотеку
    printf("Loading cache library...\n");
    if (load_cache_library() != 0) {
        fprintf(stderr, "Falling back to standard file operations\n");
        // Можно добавить fallback на стандартные операции
    }

    const char *file_name = argv[1];
    int iterations = (argc > 2) ? atoi(argv[2]) : DEFAULT_ITERATIONS;
    int data_size = (argc > 3) ? atoi(argv[3]) : DEFAULT_DATA_SIZE;

    printf("Starting EMA-JOIN benchmark with cache...\n");
    printf("Output file: %s\n", file_name);
    printf("Iterations: %d\n", iterations);
    printf("Data size: %d elements\n", data_size);

    // Открываем файл через кэширующую систему
    int cache_fd = -1;
    if (lab2_open_ptr) {
        cache_fd = lab2_open_ptr(file_name);
        if (cache_fd < 0) {
            fprintf(stderr, "Error opening file with cache, using standard open\n");
            cache_fd = open(file_name, O_WRONLY | O_CREAT | O_APPEND, 0644);
        }
    } else {
        cache_fd = open(file_name, O_WRONLY | O_CREAT | O_APPEND, 0644);
    }

    if (cache_fd < 0) {
        perror("Error opening results file");
        return EXIT_FAILURE;
    }

    // Перемещаемся в конец файла для добавления данных
    if (lab2_lseek_ptr) {
        lab2_lseek_ptr(cache_fd, 0, SEEK_END);
    } else {
        lseek(cache_fd, 0, SEEK_END);
    }

    clock_t start_time = clock();

    // Запускаем EMA-JOIN вычисления
    double final_result = perform_ema_join_calculation(iterations, data_size, file_name);

    clock_t end_time = clock();
    double duration = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    printf("EMA-JOIN calculations completed!\n");
    printf("Final result: %f\n", final_result);
    printf("Execution time: %f seconds\n", duration);

    // Записываем результаты в файл с использованием кэша
    char result_buffer[256];
    int len = snprintf(result_buffer, sizeof(result_buffer),
                      "Iterations: %d, Data size: %d, Result: %f, Time: %f sec\n",
                      iterations, data_size, final_result, duration);
    
    if (lab2_write_ptr) {
        write_with_cache(cache_fd, result_buffer, len);
        // Синхронизируем данные на диск
        lab2_fsync_ptr(cache_fd);
    } else {
        write(cache_fd, result_buffer, len);
        fsync(cache_fd);
    }

    // Закрываем файл
    if (lab2_close_ptr) {
        lab2_close_ptr(cache_fd);
    } else {
        close(cache_fd);
    }

    return EXIT_SUCCESS;
}