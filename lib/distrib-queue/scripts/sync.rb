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

      def change_script
        @change_script ||= <<~SCRIPT
          redis.log(redis.LOG_WARNING, 'begin')
          local key = KEYS[1]
          local expected_status = ARGV[1]
          local new_status = ARGV[2]
          local old_status = redis.call('get', key)

          if old_status == expected_status or (not old_status and expected_status == '') then
            redis.call('set',key, new_status)
          end
          return old_status
        SCRIPT
      end
    end
  end
end
