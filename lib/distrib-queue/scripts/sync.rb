# frozen_string_literal: true

module DistribQueue
  class Scripts
    module Sync
      def set_get_script
        @set_get_script ||= <<~SCRIPT
          local key = KEYS[1]
          local status = ARGV[1]
          local old_status = redis.call('get', key)
          redis.call('set',key, status)
          return old_status
        SCRIPT
      end
    end
  end
end
