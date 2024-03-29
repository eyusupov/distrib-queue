# frozen_string_literal: true

module DistribQueue
  class Scripts
    module Queue
      def get_from_queue_snippet
        @get_from_queue_snippet ||= <<~SNIPPET
          local item = redis.call("lpop", queue_key)
        SNIPPET
      end

      def get_leased_snippet
        @get_leased_snippet ||= <<~SNIPPET
          local leases = redis.call('hgetall', leases_key)
          for i = 1, #leases, 2 do
            local lease_key = leases[i + 1]
            if redis.call('exists', lease_key) == 0 then
              item = leases[i]
              redis.call('expire', lease_key, lease_timeout)
              break
            else
              redis.call('hdel', leases_key, lease_key)
            end
          end
        SNIPPET
      end

      def get_script
        @get_script ||= <<~SCRIPT
          local queue_key = KEYS[1]
          local leases_key = KEYS[2]
          local lease_timeout = tonumber(ARGV[1])

          #{get_from_queue_snippet}

          if (not item) and (lease_timeout > 0) then
            #{get_leased_snippet}
          end

          local leased = redis.call('hlen', leases_key)

          return item
        SCRIPT
      end

      def lease_script
        @lease_script ||= <<~SCRIPT
          local leases_key = KEYS[1]
          local lease_key = KEYS[2]
          local item = ARGV[1]
          local lease_timeout = tonumber(ARGV[2])

          redis.call('hset', leases_key, item, lease_key)
          redis.call('setex', lease_key, lease_timeout, '')
        SCRIPT
      end

      def leases_script
        @leases_script = <<~SCRIPT
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
      end
    end
  end
end
