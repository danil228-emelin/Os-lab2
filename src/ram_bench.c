#define _GNU_SOURCE
// #define USE_CUSTOM_LIB

#include "ram_bench.h"
#ifdef USE_CUSTOM_LIB
#include "ccache.h"
#endif

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#define CHUNK_SIZE 4096
#define ALPHA 0.1

// Экспоненциальное скользящее среднее
double ema(double previous, double current, double alpha) {
    return alpha * current + (1.0 - alpha) * previous;
}

// Инициализация тестовых данных
void init_data(DataPoint *data, size_t size, int seed) {
    srand(seed);
    
    for (size_t i = 0; i < size; i++) {
        data[i].timestamp = (double)i;
        data[i].value = (double)rand() / RAND_MAX * 100.0;
        data[i].id = rand() % 1000;
    }
}

// JOIN операция (имитация SQL JOIN)
void perform_join(JoinData *join_data) {
    double sum = 0.0;
    int match_count = 0;
    
    // "JOIN" по полю id - находим совпадающие записи
    for (size_t i = 0; i < join_data->size; i++) {
        for (size_t j = 0; j < join_data->size; j++) {
            if (join_data->data1[i].id == join_data->data2[j].id) {
                double product = join_data->data1[i].value * join_data->data2[j].value;
                sum += product;
                match_count++;
                
                
            }
        }
    }
    
    join_data->join_result = (match_count > 0) ? sum / match_count : 0.0;
}

// Основная функция EMA-JOIN вычислений
double perform_ema_join_calculation(int iterations, size_t data_size, const char *log_file) {
    printf("Allocating memory for %zu data points...\n", data_size);
    
    // Выделяем память для данных
    DataPoint *data1 = malloc(data_size * sizeof(DataPoint));
    DataPoint *data2 = malloc(data_size * sizeof(DataPoint));
    
    if (!data1 || !data2) {
        fprintf(stderr, "Memory allocation failed!\n");
        return 0.0;
    }
    
    // Инициализируем данные
    printf("Initializing test data...\n");
    init_data(data1, data_size, 1);
    init_data(data2, data_size, 2);
    
    JoinData join_data = {
        .data1 = data1,
        .data2 = data2,
        .size = data_size,
        .join_result = 0.0
    };
    
    double ema_result = 0.0;
    
    printf("Starting %d EMA-JOIN iterations...\n", iterations);
    
    // Основной цикл вычислений
    for (int i = 0; i < iterations; i++) {
       
        // Выполняем JOIN операцию
        perform_join(&join_data);
        
        // Применяем EMA к результату JOIN
        ema_result = ema(ema_result, join_data.join_result, ALPHA);
        
        // Прогресс для длительных вычислений
        if (iterations > 100 && i % (iterations / 10) == 0) {
            printf("Progress: %d%%, Current EMA: %f\n", 
                   (i * 100) / iterations, ema_result);
            
            // Логируем промежуточные результаты
            int fd = open(log_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                char buffer[128];
                int len = snprintf(buffer, sizeof(buffer),
                                  "Iteration %d: EMA=%f, JOIN=%f\n",
                                  i, ema_result, join_data.join_result);
                write(fd, buffer, len);
                close(fd);
            }
        }
    }
    
    printf("Final EMA result: %f\n", ema_result);
    printf("Final JOIN result: %f\n", join_data.join_result);
    
    // Сохраняем финальные результаты
    int fd = open(log_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer),
                          "FINAL: EMA=%f, JOIN=%f\n",
                          ema_result, join_data.join_result);
        write(fd, buffer, len);
        close(fd);
    }
    
    free(data1);
    free(data2);
    
    return ema_result;
}

// Сохранение результатов в файл
void save_results_to_file(const char *filename, double result, double time) {
    int fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        char buffer[128];
        int len = snprintf(buffer, sizeof(buffer),
                          "Result: %f, Time: %f sec\n", result, time);
        write(fd, buffer, len);
        close(fd);
    }
}

// Загрузка данных из файла (заглушка для совместимости)
void load_data_from_file(const char *filename, DataPoint *data, size_t size) {
    // В этой версии данные генерируются, а не загружаются из файла
    printf("Generating data instead of loading from file...\n");
    init_data(data, size, time(NULL));
}
