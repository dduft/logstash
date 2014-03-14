require "logstash/codecs/base"
require "logstash/util/charset"

# The "plain" codec is for plain text with no delimiting between events.
#
# This is mainly useful on inputs and outputs that already have a defined
# framing in their transport protocol (such as zeromq, rabbitmq, redis, etc)
class LogStash::Codecs::Xls < LogStash::Codecs::Base
  config_name "xls"
  milestone 1

  # The character encoding used in this input. Examples include "UTF-8"
  # and "cp1252"
  #
  # This setting is useful if your log files are in Latin-1 (aka cp1252)
  # or in another character set other than UTF-8.
  #
  # This only affects "plain" format logs since json is UTF-8 already.
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  public
  def register
    require "fileutils" # For mkdir_p

    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  public
  def decode(data)
    if data.is_a? Hash
      line = ""
      data[:row].each do |col|
        line << "#{col};"
      end

      event = LogStash::Event.new("message" => @converter.convert(line.rstrip))
      event.tag("eof") if data[:eof]
      event["wsname"] = data[:wsname]
    end

    yield event
  end # def decode

  public
  def encode(event)
    @on_event.call data
  end # def encode
end # class LogStash::Codecs::Plain
