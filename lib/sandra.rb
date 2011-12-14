require 'active_model'
require 'cassandra/1.0'
require 'sandra/key_validator'
require 'sandra/super_column_validator'

module Sandra

  include Sandra::Indices::InstanceMethods

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
	if self.class.autogen_keys and @attributes[self.class.key.to_s].nil?
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


  module ClassMethods

    include Sandra::Indices::ClassMethods

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

  end
end
