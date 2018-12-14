# frozen_string_literal: true

module DistribQueue
  # Redis-backed queue with support for leases.
  class Queue
    # TODO: redis.script, evalsha
    CHECK_PUT_SCRIPT = <<~SCRIPT
      local status_key = KEYS[1]
      local ignore_after_first_put = ARGV[1]
      local status = redis.call('get', status_key)

      if ignore_after_first_put == "false" or status == false then
        redis.call('set', status_key, 'receiving')
        return true
      else
        return false
      end
    SCRIPT

    GET_SCRIPT = <<~SCRIPT
      local queue_key = KEYS[1]
      local leases_key = KEYS[2]
      local status_key = KEYS[3]
      local lease_timeout = tonumber(ARGV[1])

      local item = redis.call("lpop", queue_key)
      if (not item) and (lease_timeout > 0) then
        local leases = redis.call('hgetall', leases_key)
        for i = 1, #leases, 2 do
          local lease_key = leases[i + 1]
          if redis.call('exists', lease_key) == 0 then
            item = leases[i]
            redis.call('expire', lease_key, lease_timeout)
            break
          end
        end
      end

      if not item then
        redis.call('set', status_key, 'empty', 'XX')
      end

      return item
    SCRIPT

    LEASE_SCRIPT = <<~SCRIPT
      local leases_key = KEYS[1]
      local lease_key = KEYS[2]
      local item = ARGV[1]
      local lease_timeout = tonumber(ARGV[2])

      redis.call('hset', leases_key, item, lease_key)
      redis.call('setex', lease_key, lease_timeout, '')
    SCRIPT

    LEASES_SCRIPT = <<~SCRIPT
      local leases_key = KEYS[1]

      local result = {}
      local leases = redis.call('hgetall', leases_key)
      for i = 1, #leases, 2 do
        local lease_key = leases[i + 1]
        if redis.call('exists', lease_key) == 1 then
          result[#result + 1] = leases[i]
        end
      end
      return result
    SCRIPT

    def initialize(redis,
                   name: 'default',
                   lease_timeout: nil,
                   ignore_after_first_put: false)
      @ignore_after_first_put = ignore_after_first_put
      @lease_timeout = lease_timeout
      @redis = redis
      @queue_name = name
    end

    def status
      (@redis.get(status_key) || :not_started).to_sym
    end

    def send_status(status)
      @redis.set(status_key, status)
      status
    end

    def put(*items)
      return unless receive_specs?

      @redis.lpush(queue_key, items)
      send_status('running')
      items.size == 1 ? items.first : items
    end

    def get
      @redis.eval(
        GET_SCRIPT,
        [queue_key, leases_key, status_key],
        [@lease_timeout || 0]
      ).tap { |item| lease(item) }
    end

    def expire_lease(item)
      @redis.expire(lease_key(item), -1)
    end

    def renew_lease(item)
      @redis.setex(lease_key(item), @lease_timeout, '')
    end

    def release(item)
      @redis.hdel(leases_key, item)
    end

    def size
      @redis.llen(queue_key)
    end

    def items
      @redis.lrange(queue_key, 0, -1)
    end

    def leases
      return [] unless use_lease?

      @redis.eval(LEASES_SCRIPT, [leases_key])
    end

    private

    def receive_specs?
      @redis.eval(CHECK_PUT_SCRIPT, [status_key], [@ignore_after_first_put])
    end

    def lease(item)
      return unless use_lease?

      @redis.eval(LEASE_SCRIPT, [leases_key, lease_key(item)], [item, @lease_timeout])
    end

    def leases_key
      key('leases')
    end

    def lease_key(item)
      key("leases:#{Digest::SHA2.hexdigest item.to_s}")
    end

    def status_key
      key('status')
    end

    def queue_key
      key('queue')
    end

    def leased_key
      key('leased')
    end

    def key(name)
      "#{@queue_name}:#{name}"
    end

    def use_lease?
      !@lease_timeout.nil?
    end
  end
end
