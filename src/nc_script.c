#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <ctype.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_script.h>
#include <nc_stats.h>

#define MAX_PATH_LEN 1000

static int 
split(lua_State *L) 
{
    const char *string = luaL_checkstring(L, 1);
    const char *sep = luaL_checkstring(L, 2);
    const char *token;
    int i = 1;
    lua_newtable(L);
    while ((token = strchr(string, *sep)) != NULL) {
        lua_pushlstring(L, string, (size_t)(token - string));
        lua_rawseti(L, -2, i++);
        string = token + 1;
    }
    lua_pushstring(L, string);
    lua_rawseti(L, -2, i);
    return 1;
}
 
static int 
strip(lua_State *L) 
{
    const char *front;
    const char *end;
    size_t      size;
 
    front = luaL_checklstring(L, 1, &size);
    end   = &front[size - 1];

    for ( ; size && isspace(*front) ; size-- , front++)
        ;
    for ( ; size && isspace(*end) ; size-- , end--)
        ;
    
    lua_pushlstring(L, front, (size_t)(end - front) + 1);
    return 1;
}   
    
static const luaL_Reg stringext[] = {
    {"split", split},
    {"strip", strip},
    {NULL, NULL}
};

struct replicaset* 
ffi_replicaset_new(void)
{
    int i;
    struct replicaset *rs;

    rs = nc_alloc(sizeof(struct replicaset));
    if (rs == NULL) {
        log_error("failed to allocate memory");
        return NULL;
    }

    for (i = 0; i < NC_MAXTAGNUM; i++) {
        array_init(&rs->tagged_servers[i], 2, sizeof(struct server *));
    }

    return rs;
}

void
ffi_replicaset_set_master(struct replicaset *rs, struct server *server)
{
    rs->master = server;
}

void
ffi_replicaset_add_tagged_server(struct replicaset *rs, int tag_idx, struct server *server)
{
    struct server **s = array_push(&rs->tagged_servers[tag_idx]);
    if (s == NULL) {
        log_warn("can not alloc memory");
        return;
    }
    *s = server;
}

void
ffi_replicaset_deinit(struct replicaset *rs)
{
    int i;
    for (i = 0; i < NC_MAXTAGNUM; i++) {
        /* just reset the nelem, mem can be reused */
        rs->tagged_servers[i].nelem = 0;
    }
    rs->master = NULL;
}

void
ffi_replicaset_delete(struct replicaset *rs)
{
    int i;
    for (i = 0; i < NC_MAXTAGNUM; i++) {
        /* deinit array */
        array_deinit(&rs->tagged_servers[i]);
    }
    rs->master = NULL;
    nc_free(rs);
}

struct server*
ffi_server_new(struct server_pool *pool, char *name, char *id, char *ip, int port)
{
    struct server *s;
    struct string address;
    rstatus_t status;

    s = nc_alloc(sizeof(struct server));
    if (s == NULL) {
        log_error("failed to allocate memory");
        return NULL;
    }

    s->owner = pool;
    s->idx = 0;
    s->weight = 1;
    /* set name */
    string_init(&s->name);
    string_copy(&s->name, (uint8_t*)name, (uint32_t)nc_strlen(name));
    string_init(&s->pname);
    string_copy(&s->pname, (uint8_t*)name, (uint32_t)nc_strlen(name));
    string_init(&address);
    string_copy(&address, (uint8_t*)ip, (uint32_t)nc_strlen(ip));
    /* set port */
    s->port = (uint16_t)port;

    status = nc_resolve(&address, s->port, &s->sockinfo);
    if (status != NC_OK) {
        log_error("conf: failed to resolve %.*s:%d", address.len, address.data, s->port);
        return NULL;
    }

    s->family = s->sockinfo.family;
    s->addrlen = s->sockinfo.addrlen;
    s->addr = (struct sockaddr *)&s->sockinfo.addr;

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    string_deinit(&address);

    return s;
}

rstatus_t
ffi_server_connect(struct server *server) {
    struct server_pool *pool;
    struct conn *conn;
    rstatus_t status;

    pool = server->owner;
    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ERROR;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("script: connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
        return NC_ERROR;
    }

    return NC_OK;
}

rstatus_t
ffi_server_disconnect(struct server *server)
{
    struct server_pool *pool;

    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

void
ffi_server_update_done(struct server_pool *pool) {
    pool->ffi_server_update = 1;
}

void
ffi_slots_update_done(struct server_pool *pool) {
    pool->ffi_slots_update = 1;
}

void
ffi_slots_set_replicaset(struct server_pool *pool,
                         struct replicaset *rs,
                         int left, int right)
{
    int i;

    log_debug(LOG_VVERB, "script: update slots %d-%d", left, right);

    for (i = left; i <= right; i++) {
        pool->ffi_slots[i] = rs;
    }
}

struct string
ffi_pool_get_zone(struct server_pool *pool) {
    return pool->zone;
}

struct string
ffi_pool_get_env(struct server_pool *pool) {
    return pool->env;
}

void
ffi_pool_clear_servers(struct server_pool *pool) {
    pool->ffi_server.nelem = 0;
}

void
ffi_pool_add_server(struct server_pool *pool, struct server *server) {
    struct server **s;

    s = array_push(&pool->ffi_server);
    if (s != NULL) {
        *s = server;
        log_debug(LOG_NOTICE, "prepare to add server %s", server->name.data);
    } else {
        log_warn("can not alloc memory");
    }
}

void
ffi_server_table_delete(struct server_pool *pool, const char *name)
{
    return assoc_delete(pool->server_table, name, strlen(name));
}

static int
set_lua_path(lua_State* L, const char* path)
{
    /* save the package.path var */
    char lua_path[MAX_PATH_LEN] = {'\0'};
    const char *str;
    lua_getglobal(L, "package");

    /* get field "path" from table at top of stack (-1) */
    lua_getfield(L, -1, "path");

    /* grab path string from top of stack */
    str = lua_tostring(L, -1);

    log_debug(LOG_VVVERB, "get lua package.path %s", str);

    strcat(lua_path, str);
    strcat(lua_path, ";");
    strcat(lua_path, path);
    strcat(lua_path, "/?.lua");

    /* get rid of the string on the stack we just pushed */
    lua_pop(L, 1);

    /* push the new one */
    lua_pushstring(L, lua_path);

    log_debug(LOG_VVVERB, "set lua package.path %s", lua_path);

    /* set the field "path" in table at -2 with value at top of stack */
    lua_setfield(L, -2, "path");

    /* get rid of package table from top of stack */
    lua_pop(L, 1);
    return 0;
}

/* init */
rstatus_t
script_init(struct server_pool *pool, const char *lua_path)
{
    lua_State *L;

    L = luaL_newstate();                        /* Create Lua state variable */
    pool->L = L;
    luaL_openlibs(L);                           /* Load Lua libraries */
    char path[MAX_PATH_LEN] = {'\0'};
    char *lua_name = "redis.lua";

    strcat(path, lua_path);
    strcat(path, "/");
    strcat(path, lua_name);

    set_lua_path(L, lua_path);

    if (luaL_loadfile(L, path)) {
        log_debug(LOG_VERB, "init lua script failed - %s", lua_tostring(L, -1));
        return NC_ERROR;
    }

    lua_getglobal(L, "string");
    luaL_register(L, 0, stringext);
    lua_setglobal(L, "string");

    lua_pushlightuserdata(L, pool);
    lua_setglobal(L, "__pool");

    if (lua_pcall(L, 0, 0, 0) != 0) {
        while(lua_gettop(L) > 0) {
            lua_pop(L, 1);
        }
        log_error("call lua script failed - %s", lua_tostring(L, -1));
    }

    return NC_OK;
}

void
slots_debug(struct server_pool *pool, int level)
{
    if (level > LOG_DEBUG) {
        int i = 0;
        struct replicaset *last_rs = NULL;
        for (i = 0; i < REDIS_CLUSTER_SLOTS; i++) {
            struct replicaset *rs = pool->ffi_slots[i];
            if (rs && last_rs != rs) {
                last_rs = rs;
                log_debug(LOG_VERB, "slot %5d master %.*s tags[%d,%d,%d,%d,%d]",
                        i,
                        (rs->master ? rs->master->pname.len : 3),
                        (rs->master ? (char*)rs->master->pname.data : "nil"),
                        array_n(&rs->tagged_servers[0]),
                        array_n(&rs->tagged_servers[1]),
                        array_n(&rs->tagged_servers[2]),
                        array_n(&rs->tagged_servers[3]),
                        array_n(&rs->tagged_servers[4]));
            } else if (rs == NULL && last_rs != rs) {
                last_rs = rs;
                log_debug(LOG_VERB, "slot %5d owned by no server", i);
            }
        }
    }
}

rstatus_t
script_call(struct server_pool *pool, const uint8_t *body, int len, const char *func_name)
{
    lua_State *L = pool->L;

    log_debug(LOG_VERB, "script: update redis cluster nodes");

    lua_getglobal(L, func_name);
    lua_pushlstring(L, (const char*)body, (size_t)len);

    /* Call update function */
    if (lua_pcall(L, 1, 0, 0) != 0) {
        log_warn("script: call %s failed", func_name);
        /* output and pop the error from stack */
        while(lua_gettop(L) > 0) {
            log_warn(lua_tostring(L, 1));
            lua_pop(L, 1);
        }
        return NC_ERROR;
    }
    return NC_OK;
}
