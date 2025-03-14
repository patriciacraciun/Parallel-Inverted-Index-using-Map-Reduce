#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <ctype.h>

#define ALPHABET_SIZE 26

// structura pentru un cuvant si lista fisierelor in care apare
typedef struct {
    char word[100];
    int file_ids[1000];
    int file_count;
} WordEntry;

// structura pentru rezultatele unui mapper
typedef struct {
    WordEntry *entries;
    int entry_count;
    int max_entries;
    pthread_mutex_t mutex;
} MapperResults;

// structura pentru argumentele unui mapper
typedef struct {
    char **file_list;
    int file_count;
    int mapper_id;
    int *next_file;
    pthread_mutex_t *mutex;
    MapperResults *mapper_results;
    int *mapper_files_counter;
} MapperArgs;

// coada alfabetica pentru procesarea literelor
typedef struct {
    char letters[ALPHABET_SIZE];
    int front;
    int rear;
    pthread_mutex_t mutex;
} AlphabetQueue;

// structura pentru argumentele unui reducer
typedef struct {
    MapperResults *mapper_results;
    AlphabetQueue *queue;
    int num_mappers;
    pthread_mutex_t *mutex;
    int file_count;
    int *mapper_files_counter;
    int reducer_id;
} ReducerArgs;

typedef struct {
    int mode; // 0 pentru mapper, 1 pentru reducer
    MapperArgs *mapper_args;
    ReducerArgs *reducer_args;
} ThreadArgs;

// normalizare cuvant (transformare in litere mici)
void normalize_word(char *word) {
    char *dest = word, *src = word;
    while (*src) {
        if (isalpha((unsigned char)*src)) {
            *dest++ = tolower((unsigned char)*src);
        }
        src++;
    }
    *dest = '\0';
}

// adauga un id de fisier in lista unui cuvant
void add_file_id(WordEntry *entry, int file_id) {
    for (int i = 0; i < entry->file_count; i++) {
        if (entry->file_ids[i] == file_id) {
            return;
        }
    }
    entry->file_ids[entry->file_count++] = file_id;
}

// compara doua intrari de cuvinte pentru sortare
int compare_word_entries(const void *a, const void *b) {
    WordEntry *entry_a = (WordEntry *)a;
    WordEntry *entry_b = (WordEntry *)b;

    if (entry_b->file_count != entry_a->file_count) {
        return entry_b->file_count - entry_a->file_count;
    }
    return strcmp(entry_a->word, entry_b->word);
}

// initializeaza coada alfabetica
void init_alphabet_queue(AlphabetQueue *queue) {
    for (int i = 0; i < ALPHABET_SIZE; i++) {
        queue->letters[i] = 'a' + i;
    }
    queue->front = 0;
    queue->rear = ALPHABET_SIZE - 1;
    pthread_mutex_init(&queue->mutex, NULL);
}

// preia urmatoarea litera din coada
char get_next_letter(AlphabetQueue *queue) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->front > queue->rear) {
        pthread_mutex_unlock(&queue->mutex);
        return '\0';
    }
    char letter = queue->letters[queue->front++];
    pthread_mutex_unlock(&queue->mutex);
    return letter;
}

// compara id-urile fisierelor pentru sortare
int compare_ids(const void *a, const void *b) {
    return (*(int *)a - *(int *)b);
}

// adauga un cuvant si id-ul fisierului in rezultatele mapperului
void add_word_to_results(MapperResults *results, const char *word, int file_id) {
    pthread_mutex_lock(&results->mutex);
    // verificam daca deja exista cuvantul
    for (int i = 0; i < results->entry_count; i++) {
        if (strcmp(results->entries[i].word, word) == 0) {
            add_file_id(&results->entries[i], file_id);
            pthread_mutex_unlock(&results->mutex);
            return;
        }
    }
    if (results->entry_count >= results->max_entries) {
        results->max_entries *= 2;
        results->entries = realloc(results->entries, results->max_entries * sizeof(WordEntry));
        if (!results->entries) {
            perror("error reallocating memory");
            exit(EXIT_FAILURE);
        }
    }
    //adaugam cuvant nou
    strcpy(results->entries[results->entry_count].word, word);
    results->entries[results->entry_count].file_count = 0;
    add_file_id(&results->entries[results->entry_count], file_id);
    results->entry_count++;
    pthread_mutex_unlock(&results->mutex);
}

// functia mapperului
void *mapper_function(void *args) {
    MapperArgs *mapper_args = (MapperArgs *)args;
    char word[100];
    int file_index;

    while (1) {
        // sincronizare pentru a prelua indexul urmatorului fisier
        pthread_mutex_lock(mapper_args->mutex);
        file_index = *(mapper_args->next_file);
        if (file_index >= mapper_args->file_count) {
            pthread_mutex_unlock(mapper_args->mutex);
            break;
        }
        (*(mapper_args->next_file))++;
        pthread_mutex_unlock(mapper_args->mutex);

        FILE *file = fopen(mapper_args->file_list[file_index], "r");
        if (!file) {
            perror("error opening file");
            exit(EXIT_FAILURE);
        }

        // citeste cuvintele din fisier si le adauga in structura rezultatelor
        while (fscanf(file, "%s", word) != EOF) {
            normalize_word(word);
            if (strlen(word) > 0) {
                add_word_to_results(mapper_args->mapper_results, word, file_index + 1);
            }
        }

        fclose(file);

        // actualizeaza contorul fisierelor procesate
        pthread_mutex_lock(mapper_args->mutex);
        (*(mapper_args->mapper_files_counter))++;
        pthread_mutex_unlock(mapper_args->mutex);
    }

    return NULL;
}

// functia reducerului
void *reducer_function(void *args) {
    ReducerArgs *reducer_args = (ReducerArgs *)args;

    // asteapta pana cand toate fisierele au fost procesate de mappers
    while (1) {
        pthread_mutex_lock(reducer_args->mutex);
        if (*(reducer_args->mapper_files_counter) == reducer_args->file_count) {
            pthread_mutex_unlock(reducer_args->mutex);
            break;
        }
        pthread_mutex_unlock(reducer_args->mutex);
    }

    char letter = get_next_letter(reducer_args->queue);

    while (letter != '\0') {
        WordEntry *entries = malloc(10000 * sizeof(WordEntry));
        if (!entries) {
            perror("error allocating memory");
            exit(EXIT_FAILURE);
        }

        int entry_count = 0;

        // cauta in rezultatele mapperilor cuvinte care incep cu litera curenta
        for (int i = 0; i < reducer_args->num_mappers; i++) {
            MapperResults *results = &reducer_args->mapper_results[i];
            pthread_mutex_lock(&results->mutex);
            for (int j = 0; j < results->entry_count; j++) {
                if (results->entries[j].word[0] == letter) {
                    bool found = false;
                    for (int k = 0; k < entry_count; k++) {
                        // verifica daca cuvantul exista deja in lista finala
                        if (strcmp(entries[k].word, results->entries[j].word) == 0) {
                            for (int m = 0; m < results->entries[j].file_count; m++) {
                                // adauga id-urile fisierelor
                                add_file_id(&entries[k], results->entries[j].file_ids[m]);
                            }
                            found = true;
                            break;
                        }
                    }
                    // adauga cuvantul nou in lista finala
                    if (!found) {
                        entries[entry_count] = results->entries[j];
                        entry_count++;
                    }
                }
            }
            pthread_mutex_unlock(&results->mutex);
        }

        qsort(entries, entry_count, sizeof(WordEntry), compare_word_entries);

        char output_file[10];
        snprintf(output_file, sizeof(output_file), "%c.txt", letter);
        FILE *output = fopen(output_file, "w");
        if (!output) {
            perror("error opening output file");
            free(entries);
            exit(EXIT_FAILURE);
        }

        // scrie rezultatele ordonate in fisierul corespunzator literei
        for (int i = 0; i < entry_count; i++) {
            qsort(entries[i].file_ids, entries[i].file_count, sizeof(int), compare_ids);
            fprintf(output, "%s:[", entries[i].word);
            for (int j = 0; j < entries[i].file_count; j++) {
                fprintf(output, "%d", entries[i].file_ids[j]);
                if (j < entries[i].file_count - 1) {
                    fprintf(output, " ");
                }
            }
            fprintf(output, "]\n");
        }

        fclose(output);
        free(entries);
        letter = get_next_letter(reducer_args->queue);
    }

    return NULL;
}


// functia comuna pentru mapperi si reduceri
void *thread_function(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    if (thread_args->mode == 0) { // mod mapper
        mapper_function(thread_args->mapper_args);
    } else if (thread_args->mode == 1) { // mod reducer
        reducer_function(thread_args->reducer_args);
    }
    return NULL;
}

// functia principala
int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s <numar_mapperi> <numar_reduceri> <fisier_intrare>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);
    char *input_file = argv[3];

    FILE *file = fopen(input_file, "r");
    if (!file) {
        perror("error opening input file");
        return EXIT_FAILURE;
    }

    int file_count;
    fscanf(file, "%d", &file_count);

    char **file_list = malloc(file_count * sizeof(char *));
    for (int i = 0; i < file_count; i++) {
        file_list[i] = malloc(512);
        fscanf(file, "%s", file_list[i]);
    }
    fclose(file);

    MapperResults *mapper_results = malloc(num_mappers * sizeof(MapperResults));
    for (int i = 0; i < num_mappers; i++) {
        mapper_results[i].entries = malloc(1000 * sizeof(WordEntry));
        mapper_results[i].entry_count = 0;
        mapper_results[i].max_entries = 1000;
        pthread_mutex_init(&mapper_results[i].mutex, NULL);
    }

    AlphabetQueue queue;
    init_alphabet_queue(&queue);

    pthread_t threads[num_mappers + num_reducers];
    ThreadArgs thread_args[num_mappers + num_reducers];
    MapperArgs mapper_args[num_mappers];
    ReducerArgs reducer_args[num_reducers];
    int next_file = 0;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    int mapper_files_counter = 0;

    // initializeaza thread-urile pentru mapperi
    for (int i = 0; i < num_mappers; i++) {
        mapper_args[i] = (MapperArgs){file_list, file_count, i, &next_file, &mutex, mapper_results + i, &mapper_files_counter};
        thread_args[i].mode = 0;
        thread_args[i].mapper_args = &mapper_args[i];
        thread_args[i].reducer_args = NULL;
    }

    // initializeaza thread-urile pentru reduceri
    for (int i = 0; i < num_reducers; i++) {
        reducer_args[i] = (ReducerArgs){mapper_results, &queue, num_mappers, &mutex, file_count, &mapper_files_counter, i};
        thread_args[num_mappers + i].mode = 1;
        thread_args[num_mappers + i].mapper_args = NULL;
        thread_args[num_mappers + i].reducer_args = &reducer_args[i];
    }

    for (int i = 0; i < num_mappers + num_reducers; i++) {
        pthread_create(&threads[i], NULL, thread_function, &thread_args[i]);
    }

    for (int i = 0; i < num_mappers + num_reducers; i++) {
        pthread_join(threads[i], NULL);
    }

    for (int i = 0; i < num_mappers; i++) {
        free(mapper_results[i].entries);
        pthread_mutex_destroy(&mapper_results[i].mutex);
    }
    free(mapper_results);

    for (int i = 0; i < file_count; i++) {
        free(file_list[i]);
    }
    free(file_list);

    return 0;
}