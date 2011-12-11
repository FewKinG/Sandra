require 'active_model'
require 'cassandra/1.0'
require 'sandra/key_validator'
require 'sandra/super_column_validator'

module Sandra
  def self.included(base)
    base.extend(ClassMethods)
    base.extend(ActiveModel::Naming)
    base.extend(ActiveModel::Callbacks)
    base.instance_eval do 
      alias :belongs_to :has_one
    end
    base.class_eval do
      include ActiveModel::Validations
      include ActiveModel::Conversion
      define_model_callbacks :create, :update, :save, :destroy
      define_model_callbacks :initialize, :only => :after
      attr_accessor :attributes, :new_record
      def initialize(attrs = {}, needPacking = true)
	# TODO how set the key and super column ??
	#@attributes = attrs.stringify_keys
	@attributes = {}
	attrs.stringify_keys.each do |k,v|
	  if v.is_a? String
	    type = self.class.attribute_types[k]
	    if type and needPacking
	      @attributes[k] = self.class.pack(v, type)
	    else
	      @attributes[k] = v
	    end
	  else
	    self.send("#{k}=", v)
	  end
	end
	@new_record = true
	if self.class.autogen_keys
	  @attributes[self.class.key.to_s] = SimpleUUID::UUID.new.to_s
	end
	run_callbacks :initialize
      end
      establish_connection
      @super_column_name = nil
      @indices = {}
      @autogen_keys = false
      @attribute_types = {}
      @associated_with = []
    end
  end

  def new_record?
    new_record
  end

  def persisted?
    false
  end

  def destroy
    run_callbacks :destroy do
      destroy_index_entry
      self.class.remove(attributes[self.class.key.to_s], self.class.super_column_name.nil? ? nil : attributes[self.class.super_column_name])
    end
  end

  def save
    callback_target = self.new_record? ? :create : :update
    run_callbacks callback_target do
      run_callbacks :save do
        attrs = attributes.dup
        key = attrs.delete(self.class.key.to_s)
	if self.class.super_column_name
	  sup_col_val = attrs.delete(self.class.super_column_name.to_s).to_s
	  attrs = {sup_col_val => attrs}
	end
        if key && valid?
          self.class.insert(key, attrs)
          new_record = false
	  create_index_entry
	  # Go through associations
	  # For every assoication, store destination mapping
	  # If the destination belongs to this model's class, add reverse mapping too
          true
        else
          false
        end
      end
    end
  end

  #indices << {:key => key, :sup_col => sup_col, :column_attr => column_attr}
  def create_index_entry
    # TODO Drop index entry first
    self.class.indices.each do |k,index|
      # Create index data
      data = {}
      data[:key] = attributes[self.class.key.to_s].to_s
      if self.class.super_column_name		
	data[:super_column] = attributes[self.class.super_column_name.to_s].to_s
      end
      if index[:block]
	data = index[:block].call(self).merge(data)
      end

      # Write index entry

      column_attr = (attributes[index[:column_attr].to_s] || self.send(index[:column_attr].to_s)).to_s
      col_family = "#{self.class.to_s}Index#{k.to_s.camelcase}"
      key = index[:key].nil? ? SimpleUUID::UUID.new.to_s : (attributes[index[:key].to_s] || self.send(index[:key].to_s)).to_s
      sup_col = index[:sup_col].nil? ? nil : (attributes[index[:sup_col].to_s] || self.send(index[:sup_col].to_s)).to_s

      if false and index[:key]
	# TODO complete this
	# NOTE doesn't work yet due to the cassandra gem get command not accepting arrays for subcolumn
	# Load existing index entry first to append this entry
	oldentry = if sup_col
		     self.class.connection.get(col_family, key, sup_cool, column_attr)
		   else
		     self.class.connection.get(col_family, key, column_attr)
		   end
	oldentry = Marshal.load(oldentry)

	entry_id = SimpleUUID::UUID.new.to_s
	entry = {column_attr => Marshal.dump({entry_id => data})}
      else
	entry_id = SimpleUUID::UUID.new.to_s
	entry = {column_attr => Marshal.dump(data)}
      end

      if sup_col
	self.class.connection.insert(col_family, key, {sup_col => entry})
      else
        self.class.connection.insert(col_family, key, entry)
      end
      
      #TODO Update indices index
      ind_ind_cf = "#{self.class.to_s}Indices"
      ind_ind_entry = Marshal.dump({
	:key => key,
	:sup_col => sup_col,
	:column => column_attr
      }).to_s
      if self.class.super_column_name
        self.class.connection.insert(ind_ind_cf, attributes[self.class.key.to_s].to_s, attributes[self.class.super_column_name.to_s].to_s, {col_family.to_s => ind_ind_entry})
      else
	self.class.connection.insert(ind_ind_cf, attributes[self.class.key.to_s].to_s, {col_family.to_s => ind_ind_entry})
      end
    end 
  end

  def destroy_index_entry
    ind_ind_cf = "#{self.class.to_s}Indices"
    if self.class.super_column_name
      entries = self.class.connection.get(ind_ind_cf, attributes[self.class.key.to_s], attributes[self.class.super_column_name.to_s])
    else
      entries = self.class.connection.get(ind_ind_cf, attributes[self.class.key.to_s])
    end
  end

  module ClassMethods

    def determine_type(value)
      case value.class.to_s
      when "String" then :string
      when "Float" then :double
      else :string
      end
    end

    def pack(value, type = nil)
      return nil if value.nil?
      type ||= determine_type(value)
      case type
      when :string then value.to_s
      when :double then [value.to_f].pack("G")
      else value
      end
    end

    def unpack(value, type)
      return nil if value.nil?
      case type
      when :string then value.to_s
      when :double then value.unpack("G").first
      else value
      end
    end
    
    def index_on(index_key, options = {}, &block)
      key = options[:group_by]
      sup_col = options[:sub_group]
      column_attr = index_key
      raise "Index attribute #{index_key} does not exist in #{self.to_s}" unless self.method_defined? column_attr
      raise "Index subgroup #{sup_col} does not exist in #{self.to_s}" unless sup_col.nil? or self.method_defined? sup_col
      raise "Index group #{key} does not exist in #{self.to_s}" unless key.nil? or self.method_defined? key
      validates(index_key, :presence => true)
      validates(key, :presence => true) if key
      validates(sup_col, :presence => true) if sup_col
      index_name = options[:name] || index_key
      @indices[index_name] = {:key => key, :sup_col => sup_col, :column_attr => column_attr}
      if block_given?
	@indices[index_name][:block] = block
      end
    end

    def has_one(assoc_name)
      klass = Kernel.const_get assoc_name.to_s.camelcase 
      associated_with << assoc_name

      define_method assoc_name do
	klass.get(attributes["#{assoc_name}_key"], attributes["#{assoc_name}_supercol"])	  
      end	  
      define_method "#{assoc_name}=" do |val,*args|
	nonrecurse, *ignored = args
	nonrecurse ||= false
	if val.nil?
	  preval = self.send("#{assoc_name}")
	  attributes["#{assoc_name}_key"] = ""
	  attributes["#{assoc_name}_supercol"] = ""
	  if preval and not nonrecurse and klass.associated_with.include?(self.class.to_s.underscore.to_sym)
	    preval.send("#{self.class.to_s.underscore}=", nil, true)
	    preval.save
	  end
	else
	  raise "Invalid assignment, expected #{klass}, got #{val.class}" unless val.class == klass
	  attributes["#{assoc_name}_key"] = val.attributes[klass.key.to_s]
	  if klass.super_column_name
	    attributes["#{assoc_name}_supercol"] = val.attributes[klass.super_column_name.to_s]
	  end
	  if not nonrecurse and klass.associated_with.include?(self.class.to_s.underscore.to_sym)
	    val.send("#{self.class.to_s.underscore}=", self, true)
	    val.save
	  end
	end
	save
      end	  
    end

    def column(col_name, type, options = {})
      define_method col_name do
        attr = col_name.to_s
	self.class.unpack attributes[attr], type
      end
      unless options[:getter_only]
	define_method "#{col_name}=" do |val|
	  attr = col_name.to_s
	  attributes[attr] = self.class.pack(val, type)
	end
      end
      @attribute_types[col_name.to_s] = type
    end

    def super_column(col_name, type)
      raise "#{self.to_s} already has a super column" if @super_column_name
      @super_column_name = col_name.to_s
      validates col_name, :presence => true, :super_column => true
      column col_name, type   
    end

    def establish_connection(options = {})
      connection_options = YAML.load_file("#{::Rails.root.to_s}/config/sandra.yml")[::Rails.env].merge(options)
      keyspace = connection_options["keyspace"]
      host = "#{connection_options["host"]}:#{connection_options["port"]}"
      @connection = Cassandra.new(keyspace, host)
    end

    def connection
      @connection || establish_connection
    end

    def new_object(key, attributes)
      obj = self.new(attributes.merge({@key => key}), false)
      obj.new_record = false
      obj
    end

    def insert(key, columns = {})
      connection.insert(self.to_s, key, columns)
    end

    def remove(key, super_column = nil)
      connection.remove(self.to_s, key, super_column)
    end

    def key_attribute(name, type = :string, options = {})
      raise "#{self.to_s} already has a key attribute" if @key
      @key = name
      validates name, :presence => true, :key => true
      @autogen_keys = !!options[:auto_generate]
      column name, @autogen_keys ? :string : type
    end

    def key
      @key
    end

    def super_column_name
      @super_column_name
    end

    def indices
      @indices
    end

    def attribute_types
      @attribute_types
    end

    def autogen_keys
      @autogen_keys
    end
    
    def associated_with
      @associated_with
    end

    def create(columns = {})
      obj = self.new(columns)
      obj.save
      obj
    end

    def parse_object(key, hash)
      if @super_column_name
	sup_col_val = hash.keys.first
	unless sup_col_val.nil?
	  hash = {@super_column_name => sup_col_val}.merge(hash.values.first)
	else
	  nil
	end
      end
      unless hash.empty?
        self.new_object(key, hash)
      else
        nil
      end
    end

    def get(key, super_column = nil)
      key = nil if key == ""
      super_column = nil if super_column == ""
      return nil unless key
      raise "You have to specify a super column" if not super_column and super_column_name
      raise "#{self.to_s} doesn't have a super column" if super_column and not super_column_name
      hash = connection.get(self.to_s, key, super_column)
      if super_column and hash.length > 0
	hash = {super_column => hash}
      end
      parse_object(key, hash)
    end

    def range(options = {})
      connection.get_range(self.to_s, options).map do |key, value|
	self.parse_object(key, value)
      end
    end

    def multi_get(keys = nil, options = {})
      [:start, :finish].each do |o|	
	options[o] = pack(options[o])
      end
      connection.multi_get(self.to_s, keys, options.delete(:columns), options.delete(:sub_columns), options).map do |key, value|
	self.parse_object(key, value)
      end
    end

    def destroy_all!
      connection.clear_column_family!(self.to_s)
      @indices.keys.each do |index|
	connection.clear_column_family!("#{self.to_s}Index#{index.to_s.camelcase}")
      end
    end

    def all
      range :key_count => nil, :batch_size => 50
    end

    #index_on :latitude, :group_by => :sector, :sub_group => :longitude
    # by_index :latitude, :sector => "0/0", :longitude => {:from => 0.1, :to => 0.2},
    # by_index :latitude, :sector => ["0/0", "0/1"], :longitude => 0.1, :latitude => {:from => ...
    #
    # by_index :latitude, :longitude => 0.1, :latitude => {:from => ...
    # by_index :latitude, :longitude => {:from => ...} ...
    # by_index :name, :name => "Blah"
    # by_index :name, :name => {:from => "blah", :to => "Blubb"}
    # @indices << {index_key => {:key => key, :sup_col => sup_col, :column_attr => column_attr}}
    def by_index(index_name, options = {})
      puts "Search by range"
      time = Time.now
      index = self.indices[index_name]
      raise "Index #{index_name} not existant" unless index
      family_name = "#{self.to_s}Index#{index_name.to_s.camelcase}"
      key = index[:key].nil? ? nil : options[index[:key]]
      sup_col = index[:sup_col].nil? ? nil : options[index[:sup_col]]      
      column_attr = options[index[:column_attr]]
      count = options[:count]

      if index[:sup_col]
	type = @attribute_types[index[:sup_col].to_s]
	if sup_col.is_a? Hash
	  start = pack(sup_col[:from], type)
	  finish = pack(sup_col[:to], type)
	elsif sup_col
	  start = finish = pack(sup_col, type)
	else
	  start = finish = nil
	end
      else
	type = @attribute_types[index[:column_attr].to_s]
	if column_attr.is_a? Hash
	  start = pack(column_attr[:from], type)
	  finish = pack(column_attr[:to], type)
	elsif column_attr
	  start = finish = pack(column_attr, type)
	else
	  start = finish = nil
	end
      end

      puts "Before request: #{Time.now - time}"
      if key
	# Perform multi_get
	unless key.is_a? Array
	  key = [key]
	end
	puts "Performing multi_get"
	result = connection.multi_get(family_name, key, :start => start, :finish => finish, :batch_size => 30, :type => type).values
      else
	# Perform get_range over all keys
	puts "Performing get_range"
	result = connection.get_range(family_name, :start => start, :finish => finish, :key_count => count, :batch_size => 30).values	
      end

      puts "After request: #{Time.now - time}"
      if index[:sup_col]
        if column_attr.is_a? Hash
	  start = column_attr[:from]
	  finish = column_attr[:to]
        elsif column_attr
	  start = finish = column_attr
	else
	  start = finish = nil
	end
	result = result.collect(&:values).flatten
      end
   
      puts "Until manual filtering: #{Time.now - time}" 
      if start	      
	type = @attribute_types[index[:column_attr].to_s]
	result.reject!{|entry| unpack(entry.keys.first, type) < start or unpack(entry.keys.first, type) > finish}	
      end
      
      puts "After manual filtering: #{Time.now - time}"
      if options[:index_data_only]
	result.collect{|r| Marshal.load(r.values.first)}
      elsif self.class.super_column_name
	raise "Not supported yet"
      else
	result_keys = result.collect{|r| Marshal.load(r.values.first)[:key]}
	result = []
	result_keys.each_slice(10) do |reskeys|
	  result << multi_get(reskeys, :count => nil)
	end
	result
      end
    end
  end
end
