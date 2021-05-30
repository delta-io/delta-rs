# frozen_string_literal: true

require 'spec_helper'
require 'deltalake'

describe Deltalake do
  describe '#open_table' do
    let(:table_uri) do
      File.expand_path('../rust/tests/data/simple_table')
    end

    subject(:table) { Deltalake.open_table(table_uri) }

    its(:table_uri) { should eq(table_uri) }
    its(:version) { should eq 4 }

    describe '#files' do
      subject(:files) { table.files }

      it { should be_instance_of Array }
      its(:size) { should eq(5) }

      it {
        should contain_exactly(
          'part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet',
          'part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet',
          'part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet',
          'part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet',
          'part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet'
        )
      }
    end
  end
end
