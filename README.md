Sandra
======

This gem was first developped by charlesmaxwood from teachmetocode.com .
As I'm going to use cassandra as a main data store in a current project, I found this very useful.
Therefore I'm going to extend this gem and the sandra-rails gem further to fit my needs and I will
make my changes public through git here so others may profit from it as well.

The sandra gem will provide some object-relation-mapping functionality, validations and high-level methods
to operate on the cassandra data store.
Currently, it won't care about migrations or database schemata at all, so that has still be done by hand.

But it does provide a quite nice active model abstraction layer allowing you to work with objects
stored in the cassandra database like you are used to with a relational database.
I plan to also support associations (one-to-one, one-to-many and many-to-many). 

What's already possible:
- Create a sandra model that can be loaded from and stored in the cassandra database
- Define key attribute, super column, attributes and types for that model
- Perform get, range and multi_get on the database
- Supported column types so far: String and Double
- Have the key and super column validated to prevent different objects from having the same 
  key/super column values (this is done automatically)

NOTE: If you don't use super columns then two hashes with the same keys are regarded to be the same object.
If using a super column on that model however, two hashes are regarded to be identical objects only if their keys 
AND their super column values are the same.

Also note: This gem provides no generator to actually generate a model. If you want to use a generator, have a
look into the sandra-rails gem (which I also forked to work with the 3.1.1 versions of actionpack and railties).

Example
=======

```ruby
class Example
  include Sandra
  
  key_attribute :key, :string 

  super_column :category

  column :some_value, :double
  column :text, :string
end

e = Example.new(:key => "NewKey", :category => "Cat1")
e.some_value = 1.23
e.text = "This is some text"
e.save
f = Example.get("NewKey")
fs = Example.multi_get(:keys => ["NewKey"], :start => 1.00)
```

Note that sandra will care about packing and unpacking the values (floats in this case).
