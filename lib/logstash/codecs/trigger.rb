require "logstash/codecs/base"
require "logstash/util/charset"

# The "plain" codec is for plain text with no delimiting between events.
#
# This is mainly useful on inputs and outputs that already have a defined
# framing in their transport protocol (such as zeromq, rabbitmq, redis, etc)
class LogStash::Codecs::Trigger < LogStash::Codecs::Base
    config_name "trigger"
    milestone 1

    # Set the message you which to emit for each event. This supports sprintf
    # strings.
    #
    # This setting only affects outputs (encoding of events).
    config :format, :validate => :string

    # The character encoding used in this input. Examples include "UTF-8"
    # and "cp1252"
    #
    # This setting is useful if your log files are in Latin-1 (aka cp1252)
    # or in another character set other than UTF-8.
    #
    # This only affects "plain" format logs since json is UTF-8 already.
    config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

    # The attribute/field, where the matching triggers will be saved
    config :trigger_attribute, :validate => :string, :default => "trigger"

    # The prefix for the foldername, where the files should be written
    config :trigger_folder_prefix, :validate => :string, :default => "Trigger_"

    # The format for the foldername, where the files should be written
    config :trigger_folder_format, :validate => :string, :default => "yyyyMMdd_HHmmss"  

    public
    def register
        @converter = LogStash::Util::Charset.new(@charset)
        @converter.logger = @logger
    end

    public
    def decode(data)
        yield LogStash::Event.new("message" => @converter.convert(data))        
    end # def decode

    public
    def encode(event)
        if event.include?(@trigger_attribute)

            event[@trigger_attribute].each do |trigger|

                dirname = File.dirname(event['path'])
                basename = File.basename(event['path'])

                timestamp = org.joda.time.DateTime.new(trigger[:timestamp])
                triggerfolder = @trigger_folder_prefix + timestamp.toString(@trigger_folder_format);

                path = "#{dirname}/#{triggerfolder}/#{basename}"

                @logger.debug("Path from trigger", :path => path) if @logger.debug?

                new_event = event.clone
                new_event["path"] = path

                @on_event.call(new_event)
            end
        end    
    end # def encode
end # class LogStash::Codecs::Plain
