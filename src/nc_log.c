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

static struct logger logger;
static int status;
static char tem_buf[8 * LOG_MAX_LEN];
static char wf_name[LOG_FILENAME_LEN];

void
wflog_init() {
    struct logger *l = &logger;
    char *index;
    int len = 0;

    if (l->name == NULL || !strlen(l->name)) {
        l->wf_name = NULL;
        return;
    } 

    index = l->name;
    while (*index != '\0') {
        index++;
        len ++;
    }
    memcpy(wf_name, l->name, (size_t)(len));
    l->wf_name = wf_name;
    *(wf_name + len) = '.';
    *(wf_name + len + 1) = 'w';
    *(wf_name + len + 2) = 'f';
    *(wf_name + len + 3) = '\0';

    return;
}

int
log_init(struct instance *nci)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(nci->log_level, LOG_PVERB));
    l->name = nci->log_filename;
    wflog_init();
    l->nerror = 0;

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
    
    if (l->wf_name == NULL || !strlen(l->wf_name)) {
        l->wfd = STDERR_FILENO;
    } else {
        l->wfd = open(l->wf_name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr("opening log file '%s' failed: %s", l->wf_name,
                       strerror(errno));
            return -1;
        }
    }

    l->log_buf = (char*) malloc(LOG_BUF_SIZE);
    l->wflog_buf = (char*) malloc(LOG_BUF_SIZE);
    memset(l->log_buf, 0, LOG_BUF_SIZE);
    memset(l->wflog_buf, 0, LOG_BUF_SIZE);

    l->log_buf_pos = 0;
    l->log_buf_last = 0;
    l->wflog_buf_pos = 0;
    l->wflog_buf_last = 0;

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

void
log_deinit(void)
{
    struct logger *l = &logger;

    fsync(l->fd);
    fsync(l->wfd);

    nc_free(l->log_buf);
    nc_free(l->wflog_buf);

    if ((l->fd < 0 || l->fd == STDERR_FILENO) && (l->wfd < 0 || l->wfd == STDERR_FILENO)) {
        return;
    }
    close(l->fd);
    close(l->wfd);
}

void
log_reopen(void)
{
    struct logger *l = &logger;

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
    struct logger *l = &logger;

    if (l->level < LOG_PVERB) {
        l->level++;
        log_safe("up log level to %d", l->level);
    }
}

void
log_level_down(void)
{
    struct logger *l = &logger;

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

int
log_loggable(int level)
{
    struct logger *l = &logger;

    if (level > l->level) {
        return 0;
    }

    return 1;
}


void*
log_thread_loop(void* loop)
{
    struct logger *l = &logger;
    char msg[1];
    size_t size;
    ssize_t writen; 

    for(;;){
        if (read(l->notify_fd[0], msg, 1) != 1) {
            continue;
        }
        if (l->log_buf_pos != l->log_buf_last) {
            if (l->log_buf_last < l->log_buf_pos) {
                size = LOG_BUF_SIZE - l->log_buf_pos;
                writen = write(l->fd, l->log_buf + l->log_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->log_buf_pos += (size_t)writen;
                l->log_buf_pos = l->log_buf_pos % LOG_BUF_SIZE;

                size = l->log_buf_last - l->log_buf_pos;
                writen = write(l->fd, l->log_buf + l->log_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->log_buf_pos += (size_t)writen;
                l->log_buf_pos = l->log_buf_pos % LOG_BUF_SIZE;

            } else if (l->log_buf_last > l->log_buf_pos) {
                size = l->log_buf_last - l->log_buf_pos;
                writen = write(l->fd, l->log_buf + l->log_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->log_buf_pos += (size_t)writen;
                l->log_buf_pos = l->log_buf_pos % LOG_BUF_SIZE;
            }
            fsync(l->fd);
        }
        if (l->wflog_buf_pos != l->wflog_buf_last) {
            if (l->wflog_buf_last < l->wflog_buf_pos) {
                size = LOG_BUF_SIZE - l->wflog_buf_pos;
                writen = write(l->wfd, l->wflog_buf + l->wflog_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->wflog_buf_pos += (size_t)writen;
                l->wflog_buf_pos = l->wflog_buf_pos % LOG_BUF_SIZE;

                size = l->wflog_buf_last - l->wflog_buf_pos;
                writen = write(l->wfd, l->wflog_buf + l->wflog_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->wflog_buf_pos += (size_t)writen;
                l->wflog_buf_pos = l->wflog_buf_pos % LOG_BUF_SIZE;

            } else if (l->wflog_buf_last > l->wflog_buf_pos) {
                size = l->wflog_buf_last - l->wflog_buf_pos;
                writen = write(l->wfd, l->wflog_buf + l->wflog_buf_pos, size);
                if (writen <= 0) {
                    continue;
                }
                l->wflog_buf_pos += (size_t)writen;
                l->wflog_buf_pos = l->wflog_buf_pos % LOG_BUF_SIZE;
            }
            fsync(l->wfd);
        }
    }
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

int
_log_switch(int level) 
{
    if (level <= LOG_WARN)
        return LOG_WF;
    else
        return LOG_COMMIT;
}

int
_log_write_logbuf(char *dest, size_t *pos ,size_t *last, char *buf , int len)
{
    status = 1;
    size_t size;
    size_t read = *pos;
    size_t write = *last;
    size_t length = (size_t)len;
    size_t ret;

    ret = LOG_BUF_SIZE - ((write - read + LOG_BUF_SIZE) % LOG_BUF_SIZE);
    if (length > ret -1) {
        if (ret - 1 > 0) {
            length = ret - 1;
        } else {
            length = 0;
            status = -1;
            return status;
        }
    }

    if (write + length > LOG_BUF_SIZE) {
        size = LOG_BUF_SIZE - write;
        memcpy(dest + write, buf, size);
        write += size;
        write = write % LOG_BUF_SIZE;
        length = length - size;
        buf = buf + size;

        memcpy(dest + write, buf, length);
        write += length;
        write = write % LOG_BUF_SIZE;
    } else if (write + length <= LOG_BUF_SIZE) {
        memcpy(dest + write, buf, length);
        write += length;
        write = write % LOG_BUF_SIZE;
    }

    *last = write;
    return status;
}

void
_log_write_buf(int level, char *buf, int len)
{
    if (len <= 0 && len >= LOG_MAX_LEN) {
        return;
    }

    struct logger *l = &logger;
    char *dest;
    size_t *pos;
    size_t *last;

    pthread_mutex_lock(&(l->log_mutex));
    if (_log_switch(level)) {
        dest = l->wflog_buf;
        pos = &(l->wflog_buf_pos);
        last = &(l->wflog_buf_last);
    }else {
        dest = l->log_buf;
        pos = &(l->log_buf_pos);
        last = &(l->log_buf_last);
    }
    status = _log_write_logbuf(dest, pos, last, buf, len);
    if (status != 1) {
        log_stderr("copy log to log buf failed");
    }
    if (write(l->notify_fd[1], "1", 1) != 1) {
        log_stderr("notify log thread failed");
    }
    pthread_mutex_unlock(&(l->log_mutex));
}

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

    _log_write_buf(level, buf, len);

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

    n = nc_write(STDERR_FILENO, buf, len);
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

    _log_write_buf(level, buf, len);
    if (len >= size - 1) {
        _log_write_buf(level, "\n", 1);
    }
}

void
_log_safe(int level, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size;
    char *buf = tem_buf;
    va_list args;

    if (l->fd < 0) {
        return;
    }
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    _log_level(level, buf, &len);
    len += nc_safe_snprintf(buf + len, size - len, "[.......................] ");

    va_start(args, fmt);
    len += nc_safe_vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    _log_write_buf(level, buf, len);
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
