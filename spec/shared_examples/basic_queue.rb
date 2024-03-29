# frozen_string_literal: true

require 'shared_contexts/queue'

RSpec.shared_examples 'basic queue', aggregate_failures: true do
  include_context 'queue'

  describe '#put' do
    subject! { client.put(:item1) }

    specify do
      expect(subject).to eq(:item1)
      expect(other_client.items).to eq(['item1'])
      expect(other_queue.items).to be_empty
    end

    context 'with item' do
      before { client.put(:item2) }

      specify { expect(other_client.items).to match_array(%w[item1 item2]) }
      specify { expect(other_queue.items).to be_empty }
    end

    context 'multiple items' do
      subject! { client.put(:item1, :item2) }

      specify do
        expect(other_client.items).to match_array(%w[item2 item1])
      end
    end
  end

  describe '#get' do
    subject { client.get }

    specify { expect(subject).to eq(nil) }

    context 'with item' do
      before { other_client.put(:item) }

      specify { expect(subject).to eq('item') }

      context 'after getting an item' do
        before { client.get }

        specify do
          expect(subject).to be_nil
        end

        context 'if there exists leased item' do
          let(:lease_timeout) { 999 }
          before do
            other_client.put(:item)
            other_client.get
          end

          specify do
            expect(subject).to be_nil
          end
        end
      end

      context 'with two items' do
        before do
          other_client.put(:item2)
          client.release(client.get)
        end

        specify do
          expect(subject).to eq('item')
        end
      end
      # TODO: Ordering
    end

    context 'with lease' do
      let(:lease_timeout) { 999 }

      describe 'non-expired' do
        before do
          other_client.put(:item1)
          other_client.get
        end

        specify { expect(subject).to be_nil }
      end

      describe 'expired' do
        before do
          other_client.put(:item1)
          other_client.get
          other_client.expire_lease('item1')
        end

        specify { expect(subject).to eq('item1') }
      end

      context 'with two items' do
        before do
          other_client.put(:item1)
          other_client.put(:item2)
          other_client.get
        end

        specify { expect(subject).to eq('item1') }
      end
    end
  end

  describe '#expire_lease' do
    let(:lease_timeout) { 100 }

    before { client.put(:item) }

    subject { client.expire_lease(:item) }

    before do
      client.put(:item)
      client.get
      subject
    end

    specify { expect(client.leases).to eq([]) }
  end

  describe '#renew_lease' do
    let(:lease_timeout) { 100 }

    subject { client.renew_lease(:item) }

    before do
      client.put(:item)
      client.get
      client.expire_lease(:item)
      subject
    end

    specify { expect(client.leases).to eq(['item']) }
  end

  describe '#release' do
    let(:lease_timeout) { 100 }

    subject { client.release(:item) }

    before do
      client.put(:item)
      client.get
      subject
    end

    specify do
      expect(client.leases).to be_empty
      expect(client.items).to be_empty
    end
  end

  describe '#leases' do
    let(:lease_timeout) { 100 }

    before do
      client.put(:item1)
      client.put(:item2)
      client.get
      client.get
    end

    specify { expect(client.leases).to contain_exactly('item1', 'item2') }
  end

  describe '#count' do
    subject { client.count }

    specify { expect(subject).to be_zero }
  end

  describe '#leases_count' do
    subject { client.leases_count }

    let(:lease_timeout) { 100 }

    before do
      client.put(:item1)
      client.put(:item2)
      client.get
      client.get
    end

    specify { expect(subject).to eq(2) }
  end
end
