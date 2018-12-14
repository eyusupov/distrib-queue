# frozen_string_literal: true

require 'distrib-queue/queue/scripts'

module DistribQueue
  # Redis-backed queue with support for leases.
  class Queue
    extend Scripts

    def initialize(redis,
                   name: 'default',
                   lease_timeout: nil,
                   ignore_after_first_put: false)
      @ignore_after_first_put = ignore_after_first_put
      @lease_timeout = lease_timeout
      @redis = redis
      @queue_name = name
      @keys = [leases_key, status_key, queue_key, leased_key]
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

      add_to_queue(items)
      send_status('running')
      items.size == 1 ? items.first : items
    end

    def get
      @redis.eval(self.class.get_script,
                  [queue_key, leases_key, status_key],
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

    def receive_specs?
      @redis.eval(self.class.check_put_script,
                  [status_key],
                  [@ignore_after_first_put])
    end

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
