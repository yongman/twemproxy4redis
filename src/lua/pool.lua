package.path = package.path .. ";lua/?.lua;../?.lua"

local ffi = require("ffi")
local C = ffi.C

ffi.cdef[[
      struct server;
      typedef int rstatus_t;

      void ffi_pool_clear_servers(struct server_pool *pool);
      void ffi_pool_add_server(struct server_pool *pool, struct server *server);
      struct string ffi_pool_get_env(struct server_pool *pool);

      void ffi_server_table_delete(struct server_pool *pool, const char *name);

      void ffi_slots_clear_replicasets(struct server_pool *pool);

      void ffi_server_update_done(struct server_pool *pool);
      void ffi_slots_update_done(struct server_pool *pool);
]]

local server = require("server")
local replica_set = require("replica_set")
local run_env = C.ffi_pool_get_env(__pool)

local _M = {
   server_map = {},
   replica_sets = {},

   -- for check change
   last_server_names = {},

   -- struct server_pool {}
   pool = __pool,

   -- resource pools
   _rs_pool = {},
   _se_pool = {},

   _run_env = ffi.string(run_env.data, run_env.len),
}

function _M.is_online(self)
   return self._run_env == "online"
end

function _M.fetch_server(self, config)
   local s = nil

   if #self._se_pool == 0 then
      s = server:new(config)
   else
      s = table.remove(self._se_pool, 1)
      -- check this server safe to reuse
      if s:safe_reuse() then
        -- update config
        s:update_config(config)
        s:update_raw()
      else
        -- recycle the server again and alloc a new one
        self.put_server(s)
        s = server:new()
      end
   end

   return s
end

function _M.put_server(self, s)
   table.insert(self._se_pool, s)
end

function _M.fetch_replica_set(self)
   local rs = nil

   if #self._rs_pool == 0 then
      rs = replica_set:new(self.pool)
   else
      rs = table.remove(self._rs_pool, 1)
      --pop old tagged servers
      rs:deinit()
   end

   return rs
end

function _M.put_replica_set(self, rs)
   table.insert(self._rs_pool, rs)
end

-- Public Methods

function _M.set_servers(self, configs)
   local configs = configs
   local tmp_server_map = {}

   -- Update server status
   for _, config in ipairs(configs) do
      local id = config.id

      if self.server_map[id] then
         local s = self.server_map[id]
         s:update_config(config)
         tmp_server_map[id] = s
         self.server_map[id] = nil
      else
         tmp_server_map[id] = self:fetch_server(config)
      end
   end

   -- Swap server_map
   self.server_map, tmp_server_map = tmp_server_map, self.server_map

   -- Check server list changes

   -- Drop servers that we no longer use
   for id, s in pairs(tmp_server_map) do
      C.ffi_server_table_delete(__pool, s.addr)
      self:put_server(s)
   end

   local tmp_server_names = {}
   local server_changed = false

   for _, s in pairs(self.server_map) do
      table.insert(tmp_server_names, s.addr)
      -- do server connect operation in main thread
   end

   table.sort(tmp_server_names)
   if #tmp_server_names ~= #self.last_server_names then
      server_changed = true
   elseif table.concat(tmp_server_names) ~= table.concat(self.last_server_names) then
      server_changed = true
   end

   if server_changed then
      -- Reset stats
      C.ffi_pool_clear_servers(__pool)

      for _, s in pairs(self.server_map) do
         C.ffi_pool_add_server(__pool, s.raw)
      end

      C.ffi_server_update_done(__pool)
   end

   self.last_server_names = tmp_server_names
end

function _M.build_replica_sets(self)
   local tmp_rss = {}

   -- Set masters
   for id,s in pairs(self.server_map) do
      if s:is_master() then
         local rs = self:fetch_replica_set()
         rs:set_master(s)        -- for write
         rs:add_tagged_server(s) -- for read
         table.insert(tmp_rss, rs)
      end
   end

   -- Set slaves
   for id,s in pairs(self.server_map) do
      if s:is_slave() then
         local ms = self.server_map[s.master_id]
         if ms:is_slave() then
             -- slave cascade
             ms = self.server_map[ms.master_id]
             if ms:is_slave() then
                 error("slave cascade two level")
                 return
             end
         end
         if ms ~= nil then
            local rs = ms.replica_set
            rs:add_tagged_server(s)
         end
      end
   end

   -- Swap replicasets
   self.replica_sets, tmp_rss = tmp_rss, self.replica_sets

   -- Recycle
   for _, rs in ipairs(tmp_rss) do
      self:put_replica_set(rs)
   end
end

function _M.bind_slots(self)
   for _,rs in ipairs(self.replica_sets) do
      rs:bind_slots()
   end
   C.ffi_slots_update_done(__pool)
end

return _M
