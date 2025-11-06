#ifndef RAM_BENCH_H
#define RAM_BENCH_H

#include <stddef.h>

#include <stddef.h>

// Структуры данных
typedef struct {
    double timestamp;
    double value;
    int id;
} DataPoint;

//База данных.
typedef struct {
    DataPoint *data1;
    DataPoint *data2;
    size_t size;
    double join_result;
} JoinData;

// Основные функции
double perform_ema_join_calculation(int iterations, size_t data_size, const char *log_file);
void init_data(DataPoint *data, size_t size, int seed);
void perform_join(JoinData *join_data);
double ema(double previous, double current, double alpha);

// Функции для работы с файлами
void save_results_to_file(const char *filename, double result, double time);
void load_data_from_file(const char *filename, DataPoint *data, size_t size);
#endif  // RAM_BENCH_H
