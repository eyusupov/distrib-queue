# frozen_string_literal: true

# TODO: only used keys
def redis_cleanup
  keys = redis.keys
  redis.del(keys) unless keys.empty?
end
