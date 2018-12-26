# frozen_string_literal: true

require 'distrib-queue/sync'

RSpec.describe DistribQueue::Sync, :aggregate_failures do
  let(:redis) { Redis.new }
  let(:default) { nil }
  let(:client) { described_class.new(redis, key: 'key', default: default) }
  let(:other_client) { described_class.new(redis, key: 'key', default: default) }
  let(:other_key) { described_class.new(redis, key: 'other_key', default: default) }

  around(:each) do |example|
    client.cleanup
    other_key.cleanup
    example.run
    client.cleanup
    other_key.cleanup
  end

  describe '#get' do
    subject { client.get }

    it { is_expected.to be_nil }

    context 'with default value' do
      let(:default) { :default }

      it { is_expected.to eq(default) }
    end
  end

  describe '#set' do
    let(:default) { :default }
    subject { client.set(:status) }

    it { is_expected.to eq(:status) }
    specify { expect { subject }.to change { client.get }.to(:status) }
    specify { expect { subject }.to change { other_client.get }.to(:status) }
    specify { expect { subject }.not_to change { other_key.get }.from(:default) }
  end

  describe 'wait' do
  end

  describe '#cleanup' do
    subject { client.cleanup }

    before do
      client.set(:status)
      other_key.set(:status)
    end

    specify { expect { subject }.to change { client.get }.to(nil) }
    specify { expect { subject }.not_to change { other_key.get }.from(:status) }
  end
end