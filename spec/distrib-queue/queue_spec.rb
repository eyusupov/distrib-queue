# frozen_string_literal: true

require 'distrib-queue/queue'
require 'shared_examples/basic_queue'

RSpec.describe DistribQueue::Queue, :aggregate_failures do
  let(:weight_method) { nil }
  let(:weights_key) { nil }

  it_behaves_like 'basic queue'
end
