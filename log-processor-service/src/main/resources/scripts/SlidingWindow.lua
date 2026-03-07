-- KEYS[1] = zset key
-- ARGV[1] = now (epoch seconds)
-- ARGV[2] = windowSeconds

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cutoff = now - window

-- remove old events
redis.call("ZREMRANGEBYSCORE", key, 0, cutoff)

-- add current event
redis.call("ZADD", key, now, now)

-- set TTL to window (keep key small)
redis.call("EXPIRE", key, window)

-- return count in window
return redis.call("ZCARD", key)