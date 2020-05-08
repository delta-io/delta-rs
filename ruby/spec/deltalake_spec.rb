require 'spec_helper'
require 'deltalake'

describe Deltalake do
  describe '#open_table' do
    let(:table_path) do
      File.expand_path('../../rust/tests/data/simple_table')
    end

    subject(:table) { Deltalake.open_table(table_path) }

    its(:version) { should eq 4 }
  end
end
