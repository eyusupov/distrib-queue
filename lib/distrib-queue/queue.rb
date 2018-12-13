# TODO: just a queue
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
      @redis.lpop(queue_key).tap do |item|
        send_status('empty') if item.nil? && status != :not_started
      end
    end

    def size
      @redis.llen(queue_key)
    end

    def status_key
      key('status')
    end

    def queue_key
      key('queue')
    end

    def key(name)
      "#{@queue_name}:#{name}"
    end
  end
end
