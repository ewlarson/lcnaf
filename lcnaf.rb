#!/usr/bin/ruby

require 'rubygems'
require 'nokogiri'
require 'time'
require 'date'
require 'pp'
require 'rsolr'

@@authorities = Array.new
@@current_uri = ""

# Configure for Solr
@@solr = RSolr.connect :url => 'http://localhost:8983/solr/collection1'

class Authority
  attr_accessor :scheme, :authoritative_label, :uri, :variants, :authority_type, :suggestions
  
  def initialize
    self.scheme = "LCNAF"
    self.variants = Array.new
    self.authority_type = Array.new
    self.suggestions = Array.new
    
  end
  
  def to_hash
    document_hash = Hash.new
    document_hash[:id] = self.uri
    document_hash[:source] = self.scheme
    document_hash[:variants] = self.variants
    document_hash[:record_type] = self.authority_type
    document_hash[:authoritative_label] = self.authoritative_label
    document_hash[:lccn] = self.uri.split("/").last unless self.uri.nil?
    document_hash[:suggestions]= self.build_suggestions
    document_hash[:edge_suggestions] = self.build_suggestions << self.authoritative_label
    document_hash
  end
  
  def build_suggestions
    temp_authority = self.authoritative_label
    full_name = ""
    first_name = ""
    last_prefix = ""
    last_prefix = authoritative_label.scan(/[\s*]([a-z]+)/).join
    last_prefix = last_prefix.gsub('b', '')
    full_name = authoritative_label.scan(/([\(][\D]*[\)])/).join
    full_name = full_name.delete("(")
    full_name = full_name.delete(")")
      if (temp_authority.include? '1' || '2')
        date_str = temp_authority.scan(/([\d]+|[-]|([b][\.][\s]))/).join
        date_str = date_str.gsub('b. b. ', 'b. ')
        temp_authority = temp_authority.delete '0-9/-'
      else
        date_str = ""
      end
    
      temp_authority = temp_authority.sub(/\s*\(.*\)/, '')    
    
      split_array = Array.new
      split_array = temp_authority.split(",")
      i=0
      split_array.each do |string|
        if i%2 != 0
          temp_string = string + " " + split_array.first
          temp_string = temp_string.lstrip
          if full_name != ""
            first_name = full_name.split(" ").first
            else
            first_name = string.split(" ").first
          end
          if !suggestions.include?(temp_string)
            suggestions << temp_string
            if first_name != nil
              if last_prefix != ""
                suggestions << first_name + " " + last_prefix + " " + split_array.first
              else
                suggestions << first_name + " " + split_array.first
              end
            end
            if date_str !~ /^\s*$/
              suggestions << temp_string + " (" + date_str + ")"
            end
            if full_name != ""
              if last_prefix != ""
                suggestions << full_name + " " + last_prefix + " " + split_array.first
                else
                suggestions << full_name + " " + split_array.first
              end
              if date_str !~ /^\s*$/
                if last_prefix != ""
                  suggestions << full_name + " " + last_prefix + " " + split_array.first + " (" + date_str + ")"
                  else
                  suggestions << full_name + " " + split_array.first + " (" + date_str + ")"
                end
                
              end
            end
          end
        end
        i += 1
      end
    
    suggestions.uniq
  end
end

class LcnafRdf < Nokogiri::XML::SAX::Document
  
  attr_accessor :count, :stop, :current_text, :current_authority
      
  def initialize
    self.count = 0
    self.current_text = ""
    self.stop = 30
    self.current_authority = Authority.new
  end
  
  def end_document
    puts "The document has ended"
    puts "\n\n===\n\n"
    puts "Authorities: #{self.count}"
  end
  
  def end_element(name)
    case name
    # Name
    when "ns0:authoritativeLabel" then
      self.current_authority.authoritative_label = self.current_text.strip
    end
  end
  
  def start_element(name, attributes = [])
      self.current_text = ""
    attributes.flatten!
    case name
    when "rdf:type" then
      if attributes.include?("rdf:resource")
        self.current_authority.authority_type << get_attribute_value(attributes, "rdf:resource")
      end
    when "rdf:Description" then
      if @@current_uri == ""
        @@current_uri = get_attribute_value(attributes, 'rdf:about')
      end
      if @@current_uri == get_attribute_value(attributes, "rdf:about") 
        self.current_authority.uri = get_attribute_value(attributes, "rdf:about")
      else
        @@current_uri = get_attribute_value(attributes, 'rdf:about')
        self.count += 1
        @@solr.add self.current_authority.to_hash
        self.current_authority = Authority.new
        if self.count == 1000
          @@solr.commit
          self.count = 0
        end
      end
    when "ns0:hasVariant" then
      if attributes.include?("rdf:nodeID")
        self.current_authority.variants << get_attribute_value(attributes, "rdf:nodeID")
      end
    end
  end
  
  def characters(string)
    self.current_text << string
  end
  
  def get_attribute_value(attributes, attr_name)
    if attributes.include?(attr_name)
      attributes[attributes.index(attr_name) + 1]
    else
      nil
    end
  end
end

# Create a new parser
parser = Nokogiri::XML::SAX::Parser.new(LcnafRdf.new)

# Feed the parser some XML (~42G worth)
# - http://id.loc.gov/static/data/authoritiesnames.rdfxml.madsrdf.gz
parser.parse(File.open('authoritiesnames.rdfxml.madsrdf', 'rb'))