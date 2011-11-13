require 'active_model'
require 'cassandra'
require 'sandra/key_validator'

module Sandra
  def self.included(base)
    base.extend(ClassMethods)
    base.extend(ActiveModel::Naming)
    base.extend(ActiveModel::Callbacks)
    base.class_eval do
      include ActiveModel::Validations
      include ActiveModel::Conversion
      define_model_callbacks :create, :update, :save, :destroy
      attr_accessor :attributes, :new_record
      def initialize(attrs = {})
	# TODO how set the key and super column ??
        #@attributes = attrs.stringify_keys
	@attributes = {}
	attrs.stringify_keys.each do |k,v|
	  if v.is_a? String
	    @attributes[k] = v
	  else
	    self.send("#{k}=", v)
	  end
	end
        @new_record = true
      end
      establish_connection
      @super_column_name = nil
    end
  end

  def new_record?
    new_record
  end

  def persisted?
    false
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
          true
        else
          false
        end
      end
    end
  end

  module ClassMethods
    def column(col_name, type)
      define_method col_name do
        attr = col_name.to_s
	return nil if attributes[attr].nil?
	case type
	when :double then attributes[attr].unpack("G").first
	when :string then attributes[attr].to_s
	else attributes[attr]
	end
      end
      define_method "#{col_name}=" do |val|
        attr = col_name.to_s
        attributes[attr] = case type
			   when :double then [val].pack("G")
			   when :string then val.to_s
			   else val
			   end
      end
    end

    def super_column(col_name, type)
      @super_column_name = col_name.to_s
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
      obj = self.new(attributes)
      obj.send("#{@key}=", key)
      obj.new_record = false
      obj
    end

    def insert(key, columns = {})
      connection.insert(self.to_s, key, columns)
    end

    def key_attribute(name, type)
      @key = name
      validates name, :presence => true, :key => true
      column name, type
    end

    def key
      @key
    end

    def super_column_name
      @super_column_name
    end

    def create(columns = {})
      obj = self.new(columns)
      obj.save
      obj
    end

    def parse_object(key, hash)
      if @super_column_name
	sup_col_val = hash.keys.first
	hash = {@super_column_name => sup_col_val}.merge(hash.values.first)
      end
      unless hash.empty?
        self.new_object(key, hash)
      else
        nil
      end
    end

    def get(key)
      hash = connection.get(self.to_s, key)
      parse_object(key, hash)
    end

    def range(options)
      connection.get_range(self.to_s, options).map do |key, value|
	self.parse_object(key, value)
      end
    end

    def multi_get(keys = [], options)
      options.each do |k, v|
	new_val = case v.class
		  when String then v.to_s
		  when Float then [v].pack("G")
		  else v
		  end
	options[k] = new_val
      end
      connection.multi_get(self.to_s, options.delete(:keys), options.delete(:columns), options.delete(:sub_columns), options).map do |key, value|
	self.parse_object(key, value)
      end
    end
  end
end
