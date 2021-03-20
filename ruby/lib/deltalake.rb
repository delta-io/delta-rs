# frozen_string_literal: true

require 'deltalake/version'
require 'rutie'

module Deltalake
  Rutie.new(:deltalake_ruby, lib_path: '../../target/release').init 'Init_table', __dir__

  def self.open_table(table_path)
    Table.new(table_path)
  end
end
