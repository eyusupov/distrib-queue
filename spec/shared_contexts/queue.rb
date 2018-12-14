# frozen_string_literal: true

require 'redis'

RSpec.shared_context 'queue' do
  let(:redis) { Redis.new }
  let(:options) do
    {
      ignore_after_first_put: ignore_after_first_put,
      lease_timeout: lease_timeout,
      weights_key: weights_key,
      weight: weight_method
    }.compact
  end
  # TODO: think what will happen if we have clients for same queue
  # with different options (with/no lease)
  let(:client) { described_class.new(redis, options) }
  let(:other_client) { described_class.new(redis, options) }
  let(:other_queue) { described_class.new(redis, name: 'other') }
  let(:ignore_after_first_put) { false }
  let(:lease_timeout) { nil }
end
