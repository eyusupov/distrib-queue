# frozen_string_literal: true

require 'distrib-queue/queue'

module DistribQueue
  # Weighted queue
  class WeightedQueue < Queue
    @get_from_queue_snippet = <<~SNIPPET
      local item = redis.call('zrevrange', queue_key, 0, 1)[1]
      if item ~= nil then
        redis.call('zrem', queue_key, item)
      end
    SNIPPET

    def initialize(redis,
                   name: 'default',
                   lease_timeout: nil,
                   ignore_after_first_put: false,
                   weight: nil,
                   weights_key: 'weights')
      super(redis,
            name: name,
            lease_timeout: lease_timeout,
            ignore_after_first_put: ignore_after_first_put)
      @initial_weight = 0
      @new_weight = weight || method(:default_new_weight)
      @weights_key = weights_key
    end

    def items
      @redis.zrangebyscore(queue_key, -Float::INFINITY, Float::INFINITY)
    end

    def size
      @redis.zcount(queue_key, -Float::INFINITY, Float::INFINITY)
    end

    def weights
      @redis.hgetall(weights_key).transform_values(&:to_f)
    end

    def release(item)
      old_weight = super
      @redis.hset(weights_key, item, @new_weight.call(old_weight, item))
    end

    private

    def add_to_queue(items)
      items.each { |item| @redis.zadd(queue_key, current_weight(item), item) }
    end

    def current_weight(item)
      @redis.hget(weights_key, item) || @initial_weight
    end

    def default_new_weight(_old_weight, _item)
      0
    end

    def weights_key
      key(@weights_key)
    end
  end
end
