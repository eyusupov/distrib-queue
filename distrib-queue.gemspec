require File.expand_path('lib/distrib-queue/version', __dir__)

Gem::Specification.new do |s|
  s.name = 'distrib-queue'
  s.version = DistribQueue::VERSION

  s.date = '2018-12-13'
  s.authors = [
    'Eldar Yusupov (eyusupov)'
  ]
  s.email = 'eldar@toptal.com'

  s.licenses = ['MIT']

  s.files = Dir['lib/**/*.rb']
  s.require_paths = ['lib']
  s.extra_rdoc_files = ['README.md']

  s.description = 'Atomic Redis queue with leases.'
  s.summary = s.description

  s.add_development_dependency 'pry'
  s.add_development_dependency 'pry-byebug'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rubocop'
  s.add_development_dependency 'rubocop-rspec'
  s.add_runtime_dependency 'redis'
  s.add_runtime_dependency 'rspec-core'
end
