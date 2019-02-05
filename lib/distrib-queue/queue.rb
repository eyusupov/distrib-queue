# frozen_string_literal: true

require 'distrib-queue/scripts/queue'

module DistribQueue
  # Redis-backed queue with support for leases.
  class Queue
    extend Scripts::Queue

    def initialize(redis,
                   name: 'default',
                   lease_timeout: nil)
      @lease_timeout = lease_timeout
      @redis = redis
      @queue_name = name
      @keys = [leases_key, queue_key, leased_key]
    end

    def put(*items)
      add_to_queue(items)
      items.size == 1 ? items.first : items
    end

    def get
      @redis.eval(self.class.get_script,
                  [queue_key, leases_key],
                  [@lease_timeout || 0])
            .tap { |item| lease(item) }
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

    def leases_count
      @redis.hlen(leases_key)
    end

    def count
      @redis.llen(queue_key)
    end

    def items
      @redis.lrange(queue_key, 0, -1)
    end

    def cleanup
      @redis.del(*@keys)
    end

    def leases
      return [] unless use_lease?

      @redis.eval(self.class.leases_script, [leases_key])
    end

    private

    def lease(item)
      return unless use_lease?

      @redis.eval(self.class.lease_script,
                  [leases_key, lease_key(item)],
                  [item, @lease_timeout])
    end

    def add_to_queue(items)
      @redis.lpush(queue_key, items)
    end

    def leases_key
      key('leases')
    end

    def lease_key(item)
      key("leases:#{Digest::SHA2.hexdigest item.to_s}")
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
