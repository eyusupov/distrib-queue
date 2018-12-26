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
                   weight: nil,
                   weights_key: 'weights',
                   global_weights: false)
      super(redis,
            name: name,
            lease_timeout: lease_timeout)
      @initial_weight = 0
      @new_weight = weight || method(:default_new_weight)
      @weights_key = weights_key
      @global_weights = global_weights
      @keys << weights_key
    end

    def items
      @redis.zrangebyscore(queue_key, -Float::INFINITY, Float::INFINITY)
    end

    def count 
      @redis.zcount(queue_key, -Float::INFINITY, Float::INFINITY)
    end

    def weights
      @redis.hgetall(weights_key).transform_values(&:to_f)
    end

    def release(item)
      super
      new_weight = @new_weight.call(current_weight(item), item)
      @redis.hset(weights_key, item, new_weight)
    end

    private

    def add_to_queue(items)
      items.each { |item| @redis.zadd(queue_key, current_weight(item), item) }
    end

    def current_weight(item)
      @redis.hget(weights_key, item).to_f || @initial_weight
    end

    def default_new_weight(old_weight, _item)
      old_weight
    end

    def weights_key
      @weights_key ||= global_weights ? @weights_key : key(@weights_key)
    end
  end
end
