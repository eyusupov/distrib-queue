# frozen_string_literal: true

require 'redis'
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

    it { is_expected.to eq(:default) }
    specify { expect { subject }.to change { client.get }.to(:status) }
    specify { expect { subject }.to change { other_client.get }.to(:status) }
    specify { expect { subject }.not_to change { other_key.get }.from(:default) }
  end

  describe '#change' do
    subject { client.change(:old, :new) }

    context 'with old value matching' do
      before { client.set(:old) }
      specify { expect { subject }.to change { client.get }.to(:new) }
      specify { expect(subject).to eq(:old) }
    end

    context 'with old value not maching' do
      before { client.set(:young) }
      specify { expect { subject }.not_to change { client.get }.from(:young) }
      specify { expect(subject).to eq(:young) }
    end


    context 'without old value' do
      subject { client.change(nil, :new) }

      describe 'matching' do
        specify { expect { subject }.to change { client.get }.to(:new) }
        specify { expect(subject).to be_nil }
      end

      context 'not matching' do
        before { client.set(:young) }
        specify { expect { subject }.not_to change { client.get }.from(:young) }
        specify { expect(subject).to eq(:young) }
      end
    end
  end

  describe 'wait' do
    let(:default) { :default }
    before { client.set(:new_status) }

    subject { client.wait(old_status: :default) }

    it { is_expected.to eq(:new_status) }
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
