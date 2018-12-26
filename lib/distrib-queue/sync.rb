# frozen_string_literal: true

module DistribQueue
  # Synchronization primitive. Allows to get set, get and wait for status
  class Sync
    def initialize(redis,
                   key: ,
                   default: nil)
      @redis = redis
      @key = key
      @default = default
    end

    def get
      @redis.get(@key)&.to_sym || @default
    end

    def set(status)
      @redis.set(@key, status)
      status
    end

    def wait(new_status)
    end

    def cleanup()
      @redis.del(@key)
    end
  end
end
