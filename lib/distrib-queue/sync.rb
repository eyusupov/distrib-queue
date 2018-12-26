# frozen_string_literal: true
#
require 'distrib-queue/scripts/sync'

module DistribQueue
  # Synchronization primitive. Allows to get set, get and wait for status
  class Sync
    extend Scripts::Sync

    def initialize(redis,
                   key:,
                   default: nil)
      @redis = redis
      @key = key
      @default = default
    end

    def get
      @redis.get(@key)&.to_sym || @default
    end

    def set(status)
      @redis.eval(self.class.set_get_script, [@key], [status])
        &.to_sym || @default
    end

    def wait(old_status: nil)
      # not optimal and not very testable, think of something
      status = nil
      loop do
        status = get
        break if status != old_status
        sleep 1
      end
      return status
    end

    def cleanup()
      @redis.del(@key)
    end
  end
end
