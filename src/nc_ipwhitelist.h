/***************************************************************************
 * 
 * Copyright (c) 2012 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/


#ifndef  _NC_IPWHITELIST_H_
#define  _NC_IPWHITELIST_H_

#include <sys/socket.h>
#include <netinet/in.h>
typedef struct _whitelist_t whitelist_t;
whitelist_t* load_whitelist(void);
int is_whitelist_changed(void);
void free_whitelist(whitelist_t *w);
int in_whitelist_u(char *ip);
int in_whitelist(struct in_addr addr);
int whitelist_init(const char *filename, int interval);

extern whitelist_t *whitelist;
#endif  //_NC_IPWHITELIST_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
