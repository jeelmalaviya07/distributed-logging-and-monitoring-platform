local key = KEYS[1]

local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call("HMGET", key, "tokens", "timestamp")

local tokens = tonumber(bucket[1])
local lastRefill = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    lastRefill = now
end

local delta = math.max(0, now - lastRefill)
local refill = delta * refillRate
tokens = math.min(capacity, tokens + refill)

if tokens < requested then
    redis.call("HMSET", key, "tokens", tokens, "timestamp", now)
    redis.call("EXPIRE", key, 3600)
    return 0
else
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "timestamp", now)
    redis.call("EXPIRE", key, 3600)
    return 1
end