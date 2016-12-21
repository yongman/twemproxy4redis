/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_LOG_H_
#define _NC_LOG_H_

struct logger {
    char                *name;          /* log file name */
    char                *wf_name;       /* wf log file name */
    int                 level;          /* log level */
    int                 nerror;         /* # flush mbuf error */
    int                 fd;             /* log file descriptor */
    int                 wfd;            /* log file descriptor */
    char                *log_buf;       /* log mbuf header */
    char                *wflog_buf;     /* wflog mbuf header */
    size_t              log_buf_pos;    /* log buf read cursor */
    size_t              log_buf_last;   /* log buf write cursor */
    size_t              wflog_buf_pos;  /* wflog buf read cursor*/
    size_t              wflog_buf_last; /* wflog buf write cursor */
    pthread_t           log_thread;     /* log loop thread */
    pthread_mutex_t     log_mutex;      /* buffer mutex */
    int                 notify_fd[2];   /* pipe fd to notify log thread */
};

#define LOG_SLOW    0   /* slow log don‘t conflict other log level */
#define LOG_EMERG   1   /* system in unusable */
#define LOG_ALERT   2   /* action must be taken immediately */
#define LOG_CRIT    3   /* critical conditions */
#define LOG_ERR     4   /* error conditions */
#define LOG_WARN    5   /* warning conditions */
#define LOG_NOTICE  6   /* normal but significant condition (default) */
#define LOG_INFO    7   /* informational */
#define LOG_DEBUG   8   /* debug messages */
#define LOG_VERB    9   /* verbose messages */
#define LOG_VVERB   10  /* verbose messages on crack */
#define LOG_VVVERB  11  /* verbose messages on ganga */
#define LOG_PVERB   12  /* periodic verbose messages on crack */
#define LOG_ALWAYS  13  /* log always */

#define LOG_FILENAME_LEN 256 /* max length of log message */
#define LOG_MAX_LEN (8 * 256) /* max length of log message */
#define LOG_BUF_SIZE (64 * 1024 * 1024)  /* log and wflog buf size*/

#define LOG_COMMIT  0
#define LOG_WF      1

/*
 * log_stderr   - log to stderr
 * loga         - log always
 * loga_hexdump - log hexdump always
 * log_error    - error log messages
 * log_warn     - warning log messages
 * log_panic    - log messages followed by a panic
 * ...
 * log_debug    - debug log messages based on a log level
 * log_hexdump  - hexadump -C of a log buffer
 */
#ifdef NC_DEBUG_LOG

#define log_debug(_level, ...) do {                                                     \
    if (log_loggable(_level) != 0) {                                                    \
        _log(_level, __FILE__, __LINE__, 0, __VA_ARGS__);                               \
    }                                                                                   \
} while (0)

#define log_hexdump(_level, _data, _datalen, ...) do {                                  \
    if (log_loggable(_level) != 0) {                                                    \
        _log(_level, __FILE__, __LINE__, 0, __VA_ARGS__);                               \
        _log_hexdump(_level, __FILE__, __LINE__, (char *)(_data), (int)(_datalen),      \
                     __VA_ARGS__);                                                      \
    }                                                                                   \
} while (0)

#else

#define log_debug(_level, ...)
#define log_hexdump(_level, _data, _datalen, ...)

#endif

#define log_stderr(...) do {                                                            \
    _log_stderr(LOG_ALWAYS, __VA_ARGS__);                                               \
} while (0)

#define log_safe(...) do {                                                              \
    _log_safe(LOG_WARN, __VA_ARGS__);                                                 \
} while (0)

#define log_stderr_safe(...) do {                                                       \
    _log_stderr_safe(LOG_WARN, __VA_ARGS__);                                          \
} while (0)

#define loga(...) do {                                                                  \
    _log(LOG_ALWAYS, __FILE__, __LINE__, 0, __VA_ARGS__);                               \
} while (0)

#define loga_hexdump(_data, _datalen, ...) do {                                         \
    _log(LOG_ALWAYS, __FILE__, __LINE__, 0, __VA_ARGS__);                               \
    _log_hexdump(LOG_ALWAYS, __FILE__, __LINE__, (char *)(_data), (int)(_datalen),      \
                 __VA_ARGS__);                                                          \
} while (0)                                                                             \

#define log_error(...) do {                                                             \
    if (log_loggable(LOG_ALERT) != 0) {                                                 \
        _log(LOG_ALERT, __FILE__, __LINE__, 0, __VA_ARGS__);                            \
    }                                                                                   \
} while (0)

#define log_warn(...) do {                                                              \
    if (log_loggable(LOG_WARN) != 0) {                                                  \
        _log(LOG_WARN, __FILE__, __LINE__, 0, __VA_ARGS__);                             \
    }                                                                                   \
} while (0)

#define log_panic(...) do {                                                             \
    if (log_loggable(LOG_EMERG) != 0) {                                                 \
        _log(LOG_EMERG, __FILE__, __LINE__, 1, __VA_ARGS__);                            \
    }                                                                                   \
} while (0)

#define log_slow(...) do {                                                              \
    if (log_loggable(LOG_SLOW) != 0) {                                                  \
        _log(LOG_SLOW ,__FILE__, __LINE__, 0, __VA_ARGS__);                             \
    }                                                                                   \
} while (0)

int log_init(struct instance *nci);                                   
void log_deinit(void);                                                      
void log_level_up(void);                                                    
void log_level_down(void);                                                  
void log_level_set(int level);                                             
void log_stacktrace(void);                                                  
void log_reopen(void);                                                      
int log_loggable(int level); 
void* log_thread_loop(void* loop);                                           
void _log(int level, const char *file, int line, int panic, const char *fmt, ...);     
void _log_stderr(int level, const char *fmt, ...);                                     
void _log_safe(int level, const char *fmt, ...);                                       
void _log_stderr_safe(int level, const char *fmt, ...);                                
void _log_hexdump(int level, const char *file, int line, char *data, int datalen, const char *fmt, ...);  
void _log_level(int level, char *buf , int *pos);
int _log_switch(int level);
int _log_write_logbuf(char *dest, size_t *pos ,size_t *last, char *buf , int len);
void _log_write_buf(int level, char *buf , int len, bool lock);

#endif
