module Sandra
  class SuperColumnValidator < ActiveModel::EachValidator
    def validate_each(record, attribute, value)
      if record.attributes[attribute.to_s].nil?
	record.errors[attribute] << "#{attribute.to_s} cannot be nil."
      else
	record.errors[attribute] << "#{record.send(attribute.to_s)} has been taken under this key." if record.new_record? && record.class.get(record.attributes[record.class.key.to_s], record.attributes[attribute.to_s])
      end
    end 
  end
end
