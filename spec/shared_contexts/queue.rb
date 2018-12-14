# frozen_string_literal: true

require 'redis'

RSpec.shared_context 'queue' do
  let(:redis) { Redis.new }
  let(:options) do
    {
      ignore_after_first_put: ignore_after_first_put,
      lease_timeout: lease_timeout,
      weight: weight_method,
      weights_key: weights_key,
      global_weights: global_weights
    }.compact
  end
  # TODO: think what will happen if we have clients for same queue
  # with different options (with/no lease)
  let(:client) { described_class.new(redis, options) }
  let(:other_client) { described_class.new(redis, options) }
  let(:other_queue) { described_class.new(redis, name: 'other', **options) }
  let(:ignore_after_first_put) { false }
  let(:lease_timeout) { nil }
  let(:global_weights) { nil }

  around(:each) do |example|
    client.cleanup
    other_queue.cleanup
    example.run
    client.cleanup
    other_queue.cleanup
  end
end
