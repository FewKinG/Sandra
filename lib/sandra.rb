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
        @attributes = attrs.stringify_keys
        @new_record = true
	@super_column_name = nil
      end
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
        key = attrs.delete(self.class.key)
	if self.class.super_column_name
	  attrs = {attrs.delete(self.class.super_column_name) => attrs}
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
      @attributes[col_name.to_s] = ""
    end

    def super_column(col_name, type)
      super_column_name = col_name
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

    def get(key)
      hash = connection.get(self.to_s, key)
      unless hash.empty?
        self.new_object(key, hash)
      else
        nil
      end
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

    def range(options)
      connection.get_range(self.to_s, options).map do |key, value|
        self.new_object(key, value)
      end
    end
  end
end
