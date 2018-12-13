module DistribQueue
  # Redis-backed queue with support for leases.
  class Queue
    def initialize(redis,
                   name: 'default',
                   ignore_after_first_put: false,
                   lease_timeout: nil)
      @redis = redis
      @queue_name = name
      @ignore_after_first_put = ignore_after_first_put
      @lease_timeout = lease_timeout
    end

    def status
      (@redis.get(status_key) || :not_started).to_sym
    end

    def send_status(status)
      @redis.set(status_key, status)
      status
    end

    def put(*items)
      return if status != :not_started && @ignore_after_first_put

      @redis.lpush(queue_key, items)
      send_status('running')
      items.size == 1 ? items.first : items
    end

    def get
      item = @redis.lpop(queue_key) || expired_item
      if item.nil?
        send_status('empty') if status != :not_started
        return
      end
      lease(item) if @lease_timeout
      item
    end

    def expire_lease(item)
      @redis.expire(lease_key(item), -1)
    end

    def renew_lease(item)
      @redis.setex(lease_key(item), @lease_timeout, '')
    end

    def release(item)
      @redis.hdel(leases_key, item, key)
    end

    def size
      @redis.llen(queue_key)
    end

    def items
      @redis.lrange(queue_key, 0, -1)
    end

    def leases
      @redis.hgetall(leases_key)
            .select { |_, lease_key| @redis.exists(lease_key) }
            .keys
    end

    private

    def expired_item
      return unless @lease_timeout

      @redis.hgetall(leases_key)
            .each.find { |_, key| !@redis.exists(key) }&.first
    end

    def lease(item)
      key = lease_key(item)
      @redis.hset(leases_key, item, key)
      @redis.setex(key, @lease_timeout, nil)
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
  end
end
