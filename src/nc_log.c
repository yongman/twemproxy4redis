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

#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <nc_core.h>
#include <nc_log.h>


#define LOG_EX_MAX_INTERCAL 20
#define LOG_EX_MIN_INTERCAL 1

static struct logger logger;
static int status;
static int log_up_tag;
static int log_down_tag;
static int log_reopen_tag;
static int logbuf_exintercal = 10;
static int logbuf_intercal_up;
static int logbuf_intercal_down;

/*
 *  init and deinit
 */
int
log_init(struct instance *nci)
{
    struct logger *l = &logger;
    l->level = MAX(LOG_EMERG, MIN(nci->log_level, LOG_PVERB));
    l->name = nci->log_filename;
    l->nerror = 0;
    l->exchange_failed = 0;
    l->discard_log_count=0;
    log_up_tag = 0;
    log_down_tag = 0;
    log_reopen_tag = 0;
    if (l->name == NULL || !strlen(l->name)) {
        l->fd = STDERR_FILENO;
    } else {
        l->fd = open(l->name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr("opening log file '%s' failed: %s", l->name,
                       strerror(errno));
            return -1;
        }
    }

    if (l->fd == STDERR_FILENO) {
        l->wf_name = NULL;
    } else {
        size_t len = strlen(l->name);
        l->wf_name = malloc(len + 4);
        if (l->wf_name == NULL) 
            return -1;
        memcpy(l->wf_name, l->name, (size_t)(len));
        *(l->wf_name + len) = '.';
        *(l->wf_name + len + 1) = 'w';
        *(l->wf_name + len + 2) = 'f';
        *(l->wf_name + len + 3) = '\0';
    }
    if (l->wf_name == NULL || !strlen(l->wf_name)) {
        l->wfd = STDERR_FILENO;
    } else {
        l->wfd = open(l->wf_name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->wfd < 0) {
            log_stderr("opening log file '%s' failed: %s", l->wf_name,
                       strerror(errno));
            return -1;
        }
    }

    l->accesslog_buf[0] = _log_buf_get("accesslog_buf_0");
    if (l->accesslog_buf[0] == NULL) {
        log_stderr("accesslog_buf_0 malloc failed!");
        return -1;
    } 
    l->accesslog_buf[1] = _log_buf_get("accesslog_buf_1");
    if (l->accesslog_buf[1] == NULL) {
        log_stderr("accesslog_buf_1 malloc failed!");
        return -1;
    }
    l->wflog_buf[0] = _log_buf_get("wf_buf_0");
    if (l->wflog_buf[0] == NULL) {
        log_stderr("wflog_buf_0 malloc failed!");
        return -1;
    }
    l->wflog_buf[1] = _log_buf_get("wf_buf_1");
    if (l->wflog_buf[1] == NULL) {
        log_stderr("wflog_buf_1 malloc failed!");
        return -1;
    }

    status = pipe(l->notify_fd);
    if (status != 0) {
        log_stderr("pipe failed");
        return -1;
    }

    status = fcntl(l->notify_fd[1], F_SETFL, O_NONBLOCK);
    if (status == -1) {
        log_stderr("fcntl log pipe failed");
        return -1;
    }

    pthread_mutex_t temp_mutex = PTHREAD_MUTEX_INITIALIZER;
    memcpy(&l->log_mutex, &temp_mutex, sizeof(temp_mutex));

    status = pthread_create(&l->log_thread, NULL, log_thread_loop, NULL);
    if (status) {
        log_stderr("create log thread failed");
        return -1;
    }

    return 0;
}

struct log_buf *
_log_buf_get(char* name) 
{
    struct log_buf *log_buffer;
    void *buf;

    buf = malloc(LOG_BUF_CHUN_SIZE);
    if (buf == NULL) 
        return NULL;

    log_buffer = (struct log_buf *)((size_t)buf + LOG_BUF_OFFSET);
    memcpy(log_buffer->name, name, (size_t)strlen(name));
    *(log_buffer->name + (size_t)strlen(name)) = '\0';
    log_buffer->start = (size_t)buf;
    log_buffer->pos = (size_t)buf;
    log_buffer->end = (size_t)buf + LOG_BUF_OFFSET - 1;

    return log_buffer;
}

void
log_deinit(void)
{
    struct logger *l = &logger;

    fsync(l->fd);
    fsync(l->wfd);

    if (l->fd < 0 || l->fd == STDERR_FILENO) {
        return;
    } else {
        close(l->fd);
    }

    if (l->wfd < 0 || l->wfd == STDERR_FILENO) {
        return;
    } else {
        close(l->wfd);
    }

    return;
}

/* 
 * singal handler
 */

void
log_reopen(void){
    log_reopen_tag = 1;
}

void
_log_reopen(void)
{
    struct logger *l = &logger;
    log_reopen_tag = 0;

    if (l->fd != STDERR_FILENO) {
        close(l->fd);
        l->fd = open(l->name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr_safe("reopening log file '%s' failed, ignored: %s", l->name,
                       strerror(errno));
        }
    }

    if (l->wfd != STDERR_FILENO) {
        close(l->wfd);
        l->wfd = open(l->wf_name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->wfd < 0) {
            log_stderr_safe("reopening log file '%s' failed, ignored: %s", l->wf_name,
                       strerror(errno));
        }
    }
}

void
log_level_up(void)
{
    log_up_tag = 1;
}

void
_log_level_up(void)
{
    struct logger *l = &logger;
    log_up_tag = 0;

    if (l->level < LOG_PVERB) {
        l->level++;
        log_safe("up log level to %d", l->level);
    }
}

void
log_level_down(void)
{
    log_down_tag = 1;
}

void
_log_level_down(void)
{
    struct logger *l = &logger;
    log_down_tag = 0;

    if (l->level > LOG_EMERG) {
        l->level--;
        log_safe("down log level to %d", l->level);
    }
}

void
log_level_set(int level)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(level, LOG_PVERB));
    loga("set log level to %d", l->level);
}


void
logbuf_exchange_period_up(void)
{
    logbuf_intercal_up = 1;
}

void
_logbuf_exchange_period_up(void)
{
    logbuf_intercal_up = 0;
    logbuf_exintercal++;
    logbuf_exintercal = logbuf_exintercal > LOG_EX_MAX_INTERCAL ? LOG_EX_MAX_INTERCAL : logbuf_exintercal;
    log_safe("up log_buf exchange period to %d * 100 ms", logbuf_exintercal);
}

void
logbuf_exchange_period_down(void)
{
    logbuf_intercal_down = 1;
}

void
_logbuf_exchange_period_down(void)
{
    logbuf_intercal_down = 0;
    logbuf_exintercal--;
    logbuf_exintercal = logbuf_exintercal <= LOG_EX_MIN_INTERCAL ? LOG_EX_MIN_INTERCAL : logbuf_exintercal;
    log_safe("down log_buf exchange period to %d * 100 ms", logbuf_exintercal);
}

void
log_singal_handler(void) 
{
    if (log_up_tag) {
        _log_level_up();
    }
    if (log_down_tag) {
        _log_level_down();
    }
    if (log_reopen_tag) {
        _log_reopen();
    }
    if (logbuf_intercal_up) {
        _logbuf_exchange_period_up();
    }
    if (logbuf_intercal_down) {
        _logbuf_exchange_period_down();
    }
}

void
log_stacktrace(void)
{
    struct logger *l = &logger;

    if (l->fd < 0) {
        return;
    }
    fsync(l->fd);
    nc_stacktrace_fd(l->fd);

    if (l->wfd < 0) {
        return;
    }
    fsync(l->wfd);
    nc_stacktrace_fd(l->wfd);
}

/*
 * log thread loop
 */
static char log_buf_log[LOG_MAX_LEN];
void*
log_thread_loop(void* loop)
{
    struct logger *l = &logger;
    size_t size;
    ssize_t writen;
    int len=0;
    char msg[1];


    for(;;){
        log_singal_handler();
        if (read(l->notify_fd[0], msg, 1) != 1) {
            continue;
        }
        if (l->accesslog_buf[1]->start < l->accesslog_buf[1]->pos) {
            if (l->accesslog_buf[1]->pos == l->accesslog_buf[1]->end) {
                len = nc_scnprintf(log_buf_log, LOG_MAX_LEN, "[LOG_LOG] %s is full , begin flush!\n", l->accesslog_buf[1]->name);
                writen = write(l->wfd, log_buf_log, (size_t)len);
            }
            size = l->accesslog_buf[1]->pos - l->accesslog_buf[1]->start;
            writen = write(l->fd, (void *)l->accesslog_buf[1]->start, size);
            if (writen <= 0) {
                    continue;
            }
            l->accesslog_buf[1]->pos = l->accesslog_buf[1]->start;
        }
        if (l->wflog_buf[1]->start < l->wflog_buf[1]->pos) {
            if (l->discard_log_count) {
                len = nc_scnprintf(log_buf_log, LOG_MAX_LEN, "[LOG_LOG] discard %d log items for log_buf is full\n", l->discard_log_count);
                writen = write(l->wfd, log_buf_log, (size_t)len);
                l->discard_log_count = 0;
            }
            if (l->wflog_buf[1]->pos == l->wflog_buf[1]->end) {
                len = nc_scnprintf(log_buf_log, LOG_MAX_LEN, "[LOG_LOG] %s is full , begin flush!\n", l->wflog_buf[1]->name);
                writen = write(l->wfd, log_buf_log, (size_t)len);
            }

            size = l->wflog_buf[1]->pos - l->wflog_buf[1]->start;
            writen = write(l->wfd, (void *)l->wflog_buf[1]->start, size);
            if (writen <= 0) {
                    continue;
            }
            l->wflog_buf[1]->pos = l->wflog_buf[1]->start;
        }
    }
}

/*
 * log tick task in main thread
 * could exec in  main, stats, whiteiplist thread when log_buf is full
 */
int 
log_tick_task(void)
{
    return _swap_log_buf();
}

/* 
 * 100ms per ticked
 */
void
log_cron(void) 
{
    static int count = 0;
    count++;
    if (count == logbuf_exintercal) {
        log_tick_task();
        count = 0;
    }
}
/*
 * call this function among two conditions:
 *  a.tick task per 1s
 *  b.log_buf is full
 * @return is used for condition b
 */
int
_swap_log_buf(void)
{
    struct logger *l = &logger;
    struct log_buf *temp;
    int result = 1;

    if (pthread_mutex_trylock(&(l->log_mutex))){
        l->exchange_failed++;
        return result;
    }

    if ((l->accesslog_buf[0]->start != l->accesslog_buf[0]->pos) &&
        (l->accesslog_buf[1]->start == l->accesslog_buf[1]->pos)) {
        temp = l->accesslog_buf[0];
        l->accesslog_buf[0] = l->accesslog_buf[1];
        l->accesslog_buf[1] = temp;
        result = 0;
    } 
    
    if ((l->wflog_buf[0]->start != l->wflog_buf[0]->pos) &&
        (l->wflog_buf[1]->start == l->wflog_buf[1]->pos)) {
        temp = l->wflog_buf[0];
        l->wflog_buf[0] = l->wflog_buf[1];
        l->wflog_buf[1] = temp;
        result = 0;
    } 
    
    pthread_mutex_unlock(&(l->log_mutex));

    if (write(l->notify_fd[1], "1", 1) != 1) {
        log_stderr("notify log thread failed");
    }

    return result;
}

int
log_loggable(int level)
{
    struct logger *l = &logger;

    if (level > l->level) {
        return 0;
    }

    return 1;
}

int
_log_switch(int level)
{
    if (level <= LOG_WARN)
        return LOG_WF;
    else
        return LOG_ACCESS;
}

static struct log_buf *log_buffer;
/*
 * main thread write to logbuffer 
 * other thread also write without mutex
 * maybe write dirty, but almost not
 */
void
_log_write_buf(int level, char *buf, size_t len)
{
    if (len <= 0 && len >= LOG_MAX_LEN) {
        return;
    }
    struct logger *l = &logger;
    size_t length;
    size_t max_len;

    
    while(len) {
        if (_log_switch(level)) {
            log_buffer = l->wflog_buf[0];
        } else {
            log_buffer = l->accesslog_buf[0];
        }

        if (log_buffer->pos == log_buffer->end) {
            if (!_swap_log_buf()) {
                continue;
            } else {
                l->discard_log_count++;
                break;
            }
        }

        pthread_mutex_lock(&(l->log_mutex));
        max_len = log_buffer->end - log_buffer->pos;
        if (max_len < len) {
            length = max_len;
        } else {
            length = len;
        }
        memcpy((void *)log_buffer->pos, buf, length);
        len = len - length;
        buf = buf + length;
        log_buffer->pos = log_buffer->pos + length;
        if (log_buffer->pos > log_buffer->end) {
            log_buffer->pos = log_buffer->end;
        }  
        pthread_mutex_unlock(&(l->log_mutex));
    }    
}

/*
 * log format function
 */
static char tem_buf[8 * LOG_MAX_LEN];
void
_log(int level, const char *file, int line, int panic, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size;
    char *buf = tem_buf;
    va_list args;
    struct timeval tv;

    if (l->fd < 0) {
        return;
    }

    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    _log_level(level, buf, &len);
    gettimeofday(&tv, NULL);
    buf[len++] = '[';
    len += nc_strftime(buf + len, size - len, "%Y-%m-%d %H:%M:%S.", localtime(&tv.tv_sec));
    len += nc_scnprintf(buf + len, size - len, "%03ld", tv.tv_usec/1000);
    len += nc_scnprintf(buf + len, size - len, "] %s:%d ", file, line);

    va_start(args, fmt);
    len += nc_vscnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    _log_write_buf(level, buf, (size_t)len);

    if (panic) {
        abort();
    }
}

void
_log_stderr(int level, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char *buf = tem_buf;
    va_list args;
    ssize_t n;

    errno_save = errno;
    len = 0;                /* length of output buffer */
    size = 4 * LOG_MAX_LEN; /* size of output buffer */

    _log_level(level, buf, &len);
    va_start(args, fmt);
    len += nc_vscnprintf(buf, size, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(STDERR_FILENO, buf, (size_t)len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

/*
 * Hexadecimal dump in the canonical hex + ascii display
 * See -C option in man hexdump
 */
void
_log_hexdump(int level, const char *file, int line, char *data, int datalen,
             const char *fmt, ...)
{
    struct logger *l = &logger;
    char *buf = tem_buf;
    int i, off, len, size;

    if (l->fd < 0) {
        return;
    }

    /* log hexdump */
    off = 0;                  /* data offset */
    len = 0;                  /* length of output buffer */
    size = 8 * LOG_MAX_LEN;   /* size of output buffer */

    _log_level(level, buf, &len);
    while (datalen != 0 && (len < size - 1)) {
        char *save, *str;
        unsigned char c;
        int savelen;

        len += nc_scnprintf(buf + len, size - len, "%08x  ", off);

        save = data;
        savelen = datalen;

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(*data);
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "%02x%s", c, str);
        }
        for ( ; i < 16; i++) {
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "  %s", str);
        }

        data = save;
        datalen = savelen;

        len += nc_scnprintf(buf + len, size - len, "  |");

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(isprint(*data) ? *data : '.');
            len += nc_scnprintf(buf + len, size - len, "%c", c);
        }
        len += nc_scnprintf(buf + len, size - len, "|\n");

        off += 16;
    }

    _log_write_buf(level, buf, (size_t)len);
    if (len >= size - 1) {
        _log_write_buf(level, "\n", 1);
    }
}

void
_log_safe(int level, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char *buf = tem_buf;
    va_list args;

    if (l->fd < 0) {
        return;
    }
    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    _log_level(level, buf, &len);
    len += nc_safe_snprintf(buf + len, size - len, "[.......................] ");

    va_start(args, fmt);
    len += nc_safe_vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

	_log_write_buf(level, buf, (size_t)len);

    errno = errno_save;
}

void
_log_stderr_safe(int level, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char *buf = tem_buf;
    va_list args;
    ssize_t n;

    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    _log_level(level, buf, &len);
    len += nc_safe_snprintf(buf + len, size - len, "[.......................] ");

    va_start(args, fmt);
    len += nc_safe_vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(STDERR_FILENO, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

void
_log_level(int level, char *buf , int *pos)
{
    int len = *pos;
    int size = LOG_MAX_LEN;

    switch (level) {
        case LOG_SLOW :
            len += nc_scnprintf(buf + len, size - len, "%s", "[SLOW]");
            *pos = len;
            break;
        case LOG_EMERG :
            len += nc_scnprintf(buf + len, size - len, "%s", "[PANIC]");
            *pos = len;
            break;
        case LOG_ALERT :
            len += nc_scnprintf(buf + len, size - len, "%s", "[ERROR]");
            *pos = len;
            break;
        case LOG_CRIT :
            len += nc_scnprintf(buf + len, size - len, "%s", "[CRIT]");
            *pos = len;
            break;
        case LOG_ERR :
            len += nc_scnprintf(buf + len, size - len, "%s", "[ERCON]");
            *pos = len;
            break;
        case LOG_WARN :
            len += nc_scnprintf(buf + len, size - len, "%s", "[WARN]");
            *pos = len;
            break;
        case LOG_NOTICE :
            len += nc_scnprintf(buf + len, size - len, "%s", "[NOTICE]");
            *pos = len;
            break;
        case LOG_INFO :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        case LOG_DEBUG :
            len += nc_scnprintf(buf + len, size - len, "%s", "[DEBUG]");
            *pos = len;
            break;
        case LOG_VERB :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        case LOG_VVERB :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        case LOG_VVVERB :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        case LOG_PVERB :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        case LOG_ALWAYS :
            len += nc_scnprintf(buf + len, size - len, "%s", "[INFO]");
            *pos = len;
            break;
        default:
            len += nc_scnprintf(buf + len, size - len, "%s", "[]");
            *pos = len;
        }

    return;
}