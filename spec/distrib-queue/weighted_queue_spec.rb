# frozen_string_literal: true

require 'distrib-queue/weighted_queue'
require 'shared_examples/basic_queue'

RSpec.describe DistribQueue::WeightedQueue, :aggregate_failures do
  include_context 'queue'

  # largest go first, for compatibility with basic queue tests
  let(:weight_method) { ->(_, new_weight, _) { new_weight } }
  let(:weights_key) { nil }

  context 'default weight' do
    let(:weight_method) { nil }
    it_behaves_like 'basic queue'
  end

  describe '#weights' do
    subject { client.weights }

    before do
      client.put(:item1)
      client.release(:item1)
    end

    specify { expect(client.weights).to have_key('item1') }
  end

  describe '#release' do
    subject { client.release }

    before do
      client.put(:item10)
      client.release(:item10, 10)
    end

    specify { expect(client.weights).to eq('item10' => 10.0) }
  end

  def populate_weights
    other_client.put('item1')
    other_client.release('item1', 1)
    other_client.put('item2')
    other_client.release('item2', 2)
  end

  describe '#get' do
    let(:weight_method) do
      # Reverse the fetch order for the second iteration
      ->(old_weight, new_weight, _) { old_weight.zero? ? -new_weight : -old_weight }
    end

    context 'with weight' do
      subject { client.get }

      context 'after assigning weight' do
        before do
          populate_weights
          other_client.put('item1')
          other_client.put('item2')
        end

        specify { expect(subject).to eq('item1') }

        context 'second item' do
          before { client.get }
          specify { expect(subject).to eq('item2') }
        end
      end

      context 'with old weight' do
        before do
          populate_weights
          populate_weights
          other_client.put('item1')
          other_client.put('item2')
        end

        specify { expect(subject).to eq('item2') }

        context 'second item' do
          before { client.get }
          specify { expect(subject).to eq('item1') }
        end
      end
    end
  end

  context 'sharing weights between queues' do
    let(:global_weights) { true }
    let(:weights_key) { 'global_weights_table' }
    let(:weight_method) do
      proc do |_, item|
        -item.to_s.gsub(/^item/, '').to_i
      end
    end

    subject { other_queue.get }

    before do
      populate_weights
      other_queue.put('item1')
      other_queue.put('item2')
    end

    specify { expect(subject).to eq('item1') }

    context 'second item' do
      before { other_queue.get }
      specify { expect(subject).to eq('item2') }
    end
  end
end
