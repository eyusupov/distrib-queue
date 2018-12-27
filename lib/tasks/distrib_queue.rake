# frozen_string_literal: true

require 'redis'
require 'distrib-queue/sync'

def redis_url
  ENV['redis_url'] || ENV['DISTRIB_QUEUE_REDIS_URL'] || 'redis://localhost:6379'
end

def redis
  @redis ||= Redis.new(url: redis_url)
end

def sync
  @sync ||= DistribQueue::Sync.new(redis, key: key, default: default)
end

def key
  ENV['key'] || ENV['DISTRIB_QUEUE_SYNC_KEY']
end

def default
  ENV['default'] || ENV['DISTRIB_QUEUE_SYNC_DEFAULT_VALUE']
end

namespace :distrib_queue do
  namespace :sync do
    desc 'Get sync value'
    task :get do
      puts sync.get
    end

    desc 'Set sync value'
    task :set do
      puts sync.set(ENV['status'])
    end

    desc 'Wait for sync value to change'
    task :wait do
      puts sync.wait(old_status: ENV['old_status'])
    end
  end
end
