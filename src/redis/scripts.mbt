// The two Lua scripts that require atomic multi-key operations.
// All other lifecycle operations use native Redis Stream commands (XREADGROUP, XACK, XAUTOCLAIM).

// move_to_stream.lua
// ------------------
// Atomically pops the highest-priority job from the available sorted set and
// XADDs it to the delivery stream. Prevents two producers from racing on the
// same job between ZPOPMIN and XADD.
//
// KEYS[1] — available sorted set  (tide:queue:{name}:available)
// KEYS[2] — stream key             (tide:queue:{name}:stream)
// KEYS[3] — job hash key prefix    (tide:job:)
// ARGV[1] — number of jobs to move (integer string)
//
// Returns: array of job IDs moved into the stream.
pub let move_to_stream_script : String =
  #|local count = tonumber(ARGV[1]) or 1
  #|local moved = {}
  #|for i = 1, count do
  #|  local items = redis.call('ZPOPMIN', KEYS[1], 1)
  #|  if #items == 0 then break end
  #|  local job_id = items[1]
  #|  -- Mark executing in the hash so it's visible before the consumer group ACKs.
  #|  redis.call('HSET', KEYS[3] .. job_id, 'state', 'executing')
  #|  -- Push job_id into the stream; XREADGROUP consumers will claim it.
  #|  redis.call('XADD', KEYS[2], '*', 'job_id', job_id)
  #|  table.insert(moved, job_id)
  #|end
  #|return moved

// unique_insert.lua
// -----------------
// Checks whether a uniqueness key already exists. If not, inserts the job hash,
// sets the unique key with a TTL, and adds the job to the available sorted set.
// All three operations are atomic — no Lua-external race possible.
//
// KEYS[1] — unique key             (tide:unique:{hash})
// KEYS[2] — job hash key           (tide:job:{id})
// KEYS[3] — available sorted set   (tide:queue:{name}:available)
// ARGV[1] — unique key TTL in ms   (integer string)
// ARGV[2] — job_id                 (string)
// ARGV[3] — sorted set score       (integer string: priority * 1e12 + scheduled_at_ms)
// ARGV[4..N] — alternating hash field, value pairs
//
// Returns: "inserted" if the job was new, "exists" if a duplicate was detected.
pub let unique_insert_script : String =
  #|if redis.call('EXISTS', KEYS[1]) == 1 then
  #|  return 'exists'
  #|end
  #|redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[1])
  #|local hash_args = {}
  #|for i = 4, #ARGV do
  #|  table.insert(hash_args, ARGV[i])
  #|end
  #|redis.call('HSET', KEYS[2], unpack(hash_args))
  #|redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
  #|return 'inserted'

// Loaded SHAs after SCRIPT LOAD. Created by RedisEngine::ensure_scripts.
pub struct ScriptShas {
  move_to_stream : String
  unique_insert : String
}

pub fn ScriptShas::create(
  move_to_stream : String,
  unique_insert : String,
) -> ScriptShas {
  { move_to_stream, unique_insert }
}
