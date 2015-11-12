/***************************************************************************
 * 
 * Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

#include <pthread.h>
#include <arpa/inet.h>
#include <nc_ipwhitelist.h>
#include <nc_core.h>
#include <sys/stat.h>

static const char *whitelist_file = NULL;
static int check_interval;

typedef struct _whitelist_t {
    struct hash_table *ht;
    long mtime;
} whitelist_t;
pthread_t whitelist_thread;

whitelist_t *whitelist = NULL;

static long get_mtime(const char* filename)
{
    struct stat buf;
    if(lstat(filename, &buf)<0){
        return -1;
    }
    return (long)buf.st_mtime;
}

whitelist_t* load_whitelist(void) {
    FILE *f;
    char buf[128];
    char *line;
    char *end;
    long mtime;
    mtime = get_mtime(whitelist_file);
    if (mtime < 0) {
        return NULL;
    }
    f = fopen(whitelist_file, "r");
    if (f == NULL) {
        log_warn("Open whitelist file %s error, errmsg: %s", whitelist_file, strerror(errno));
        return NULL;
    }

    whitelist_t *w = (whitelist_t*) nc_alloc(sizeof(whitelist_t));
    if (w == NULL) {
        log_warn("malloc failed");
        return NULL;
    }
    w->ht = assoc_create_table_default();
    if (w->ht == NULL) {
        nc_free(w);
        log_warn("hashset create failed");
        return NULL;
    }
    w->mtime = mtime;

    while(fgets(buf, sizeof(buf), f) != NULL) {
        line = buf;
        //trim leading whitespace
        while (*line == ' ' || *line == '\t') line++;
        //skip empty line or comments
        if (line[0] == '#' || line[0] == '\r' || line[0] == '\n' || line[0] == 0) continue;
        end = line;
        //trim trailing characters
        while ((*end >= '0' && *end <= '9') || *end == '.') end++;
        *end = 0;

        if (strlen(line) == 0) continue;

        //add to ht
        if (assoc_set(w->ht, line, strlen(line), (void *)1) != NC_OK) {
            free_whitelist(w);
            return NULL;
        }
        log_debug(LOG_DEBUG, "whitelist added for %s", line);
    }
    fclose(f);
    return w;
}

int is_whitelist_changed(void) {
    long mtime = get_mtime(whitelist_file);
    static int flag = 0;
    if (mtime < 0 && flag == 0) {
        log_warn("Get mtime of whitelist file failed, possibly file does not exist");
        flag = 1;
    } else if (mtime > 0) {
        flag = 0;
    }

    /* none -> none: do not need reload */
    if (whitelist == NULL && mtime < 0) {
        return 0;
    }
    /* none -> have: need check */
    if (whitelist == NULL && mtime > 0) {
        return 1;
    }
    /* have -> none: need check */
    if (whitelist != NULL && mtime < 0) {
        return 1;
    }

    /* have -> have: need check */
    if (whitelist != NULL && mtime > whitelist->mtime) {
        return 1;
    }
    return 0;
}

void free_whitelist(whitelist_t *w) {
    if (!w) return;
    assoc_destroy_table(w->ht);
    nc_free(w);
}

int in_whitelist_u(char *ip) {
    if (whitelist == NULL) return 1;
    if (assoc_find(whitelist->ht, ip, nc_strlen(ip)) != NULL) {
        return 1;
    }
    return 0;
}

int in_whitelist(struct in_addr in) {
    return in_whitelist_u(inet_ntoa(in));
}

void *whitelist_loop() {
    log_debug(LOG_DEBUG, "whitelist_loop_started");
    for(;;) {

        sleep((unsigned)check_interval);
        if (is_whitelist_changed()) {
            log_warn("whitelist changed");
            whitelist_t *w = load_whitelist();
            whitelist_t *tmp = whitelist;
            whitelist = w;

            /* if whitelist changed, sleep double time */
            sleep((unsigned)check_interval);
            free_whitelist(tmp);
        }
    }
    return NULL;
}

int whitelist_init(const char *filename, int interval) {
    int ret;
    whitelist_file = filename;
    check_interval = interval;
    whitelist = load_whitelist();
    ret = pthread_create(&whitelist_thread, NULL, whitelist_loop, NULL);
    if (ret) {
        log_warn("Error create whitelist check loop thread, errstr: %s", strerror(errno));
        return -1;
    }
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
