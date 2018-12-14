require 'redis'
require 'distrib-queue/queue'

RSpec.describe DistribQueue::Queue, :aggregate_failures do
  def cleanup
    keys = redis.keys
    redis.del(keys) unless keys.empty?
  end

  around(:each) do |example|
    cleanup
    # TODO: only used keys
    example.run
    cleanup
  end

  let(:redis) { Redis.new }
  let(:options) do
    {
      ignore_after_first_put: ignore_after_first_put,
      lease_timeout: lease_timeout
    }
  end
  # TODO: think what will happen if we have clients for same queue
  # with different options (with/no lease)
  let(:client) { described_class.new(redis, options) }
  let(:other_client) { described_class.new(redis, options) }
  let(:other_queue) { described_class.new(redis, name: 'other') }
  let(:ignore_after_first_put) { false }
  let(:lease_timeout) { nil }

  describe '#status' do
    specify do
      expect(client.status).to eq(:not_started)
      expect(other_client.status).to eq(:not_started)
      expect(other_queue.status).to eq(:not_started)
    end
  end

  describe '#status=' do
    subject! { client.send_status(:running) }

    specify do
      expect(subject).to eq(:running)
      expect(other_client.status).to eq(:running)
    end
  end

  describe '#put' do
    subject! { client.put(:item1) }

    specify do
      expect(subject).to eq(:item1)
      expect(other_client.items).to eq(['item1'])
      expect(other_client.status).to eq(:running)
      expect(other_queue.status).to eq(:not_started)
      expect(other_queue.items).to be_empty
    end

    context 'with item' do
      before { client.put(:item2) }

      specify { expect(other_client.items).to eq(%w[item2 item1]) }
      specify { expect(other_queue.items).to be_empty }

      context 'after getting an item' do
        before { other_client.get }

        specify { expect(other_client.items).to eq(['item1']) }
      end
    end

    context 'with ignore after first put' do
      let(:ignore_after_first_put) { true }

      specify do
        expect { other_client.put(:item2) }
          .not_to change { other_client.size }.from(1)
      end
    end

    context 'multiple items' do
      subject! { client.put(:item1, :item2) }

      specify do
        expect(other_client.items).to eq(%w[item2 item1])
      end

      context 'with ignore after first put' do
        let(:ignore_after_first_put) { true }

        specify do
          expect { other_client.put(:item3, :item4) }
            .not_to change { other_client.size }.from(2)
        end
      end
    end
  end

  describe '#get' do
    subject { client.get }

    specify { expect(subject).to eq(nil) }
    specify do
      expect { subject }.not_to change { client.status }.from(:not_started)
    end

    context 'with item' do
      before { other_client.put(:item) }

      specify { expect(subject).to eq('item') }

      context 'after getting an item' do
        before { client.get }

        specify do
          expect(subject).to be_nil
          expect(client.status).to eq(:empty)
        end
      end
    end

    context 'with non-expired lease' do
      let(:lease_timeout) { 999 }

      before do
        other_client.put(:item1)
        other_client.get
      end

      specify { expect(subject).to be_nil }
    end

    context 'with expired lease' do
      let(:lease_timeout) { 100 }

      before do
        other_client.put(:item1)
        other_client.get
        other_client.expire_lease('item1')
      end

      specify { expect(subject).to eq('item1') }
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

  describe '#size' do
    subject { client.size }

    specify { expect(subject).to be_zero }
  end
end
