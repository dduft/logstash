require "logstash/inputs/base"
require "logstash/namespace"

require "pathname"
require "socket" # for Socket.gethostname

# Stream events from files.
#
# By default, each event is assumed to be one line. If you
# want to join lines, you'll want to use the multiline filter.
#
# Files are followed in a manner similar to "tail -0F". File rotation
# is detected and handled by this input.
class LogStash::Inputs::File2 < LogStash::Inputs::Base
  config_name "file2"
  milestone 1

  # TODO(sissel): This should switch to use the 'line' codec by default
  # once file following
  default :codec, "line"

  # The path to the file to use as an input.
  # You can use globs here, such as `/var/log/*.log`
  # Paths must be absolute and cannot be relative.
  config :path, :validate => :array, :required => true

  # Exclusions (matched against the filename, not full path). Globs
  # are valid here, too. For example, if you have
  #
  #     path => "/var/log/*"
  #
  # you might want to exclude gzipped files:
  #
  #     exclude => "*.gz"
  config :exclude, :validate => :array

  # How often we stat files to see if they have been modified. Increasing
  # this interval will decrease the number of system calls we make, but
  # increase the time to detect new log lines.
  config :stat_interval, :validate => :number, :default => 1

  # How often we expand globs to discover new files to watch.
  config :discover_interval, :validate => :number, :default => 15

  # Where to write the since database (keeps track of the current
  # position of monitored log files). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  config :sincedb_path, :validate => :string, :required => true

  # How often to write a since database with the current position of
  # monitored log files.
  config :sincedb_write_interval, :validate => :number, :default => 15

  # Choose where logstash starts initially reading files - at the beginning or
  # at the end. The default behavior treats files like live streams and thus
  # starts at the end. If you have old data you want to import, set this
  # to 'beginning'
  #
  # This option only modifieds "first contact" situations where a file is new
  # and not seen before. If a file has already been seen before, this option
  # has no effect.
  config :start_position, :validate => [ "beginning", "end"], :default => "beginning"

  # Should the progressdb events be send to the pipeline
  config :progressdb, :validate => :boolean, :default => false

  # Should the processdb entry be deleted after file-deletion
  config :progressdb_del, :validate => :boolean, :default => false

  # Close the file when end is reached
  # This make sense when reading a file once from the beginning and want to e.g.
  # proceed renaming or deleting the parent folder
  config :eof_close, :validate => :boolean, :default => false

  public
  def register
    require "addressable/uri"
    require "filewatch/filetail"
    require "digest/md5"
    @logger.info("Registering file input", :path => @path)

    @tail_config = {
      :exclude => @exclude,
      :stat_interval => @stat_interval,
      :discover_interval => @discover_interval,
      :sincedb_write_interval => @sincedb_write_interval,
      :logger => @logger,
      :progressdb => @progressdb,
      :progressdb_del => @progressdb_del,
      :eof_close => @eof_close,
    }

    @path.each do |path|
      if Pathname.new(path).relative?
        raise ArgumentError.new("File paths must be absolute, relative path specified: #{path}")
      end
    end

    @tail_config[:sincedb_path] = @sincedb_path

    if @start_position == "beginning"
      @tail_config[:start_new_files_at] = :beginning
    end

    @codec_plain = LogStash::Codecs::Plain.new
  end # def register

  public
  def run(queue)
    @tail = FileWatch::FileTail.new(@tail_config)
    @tail.logger = @logger
    @path.each { |path| @tail.tail(path) }
    hostname = Socket.gethostname

    @tail.subscribe do |path, data, type|
      @logger.debug("Received line", :path => path, :data => data) if logger.debug?

      if type == :log
        @codec.decode(data) do |event|

          decorate(event)
          #event["tags"] ||= []
          event["host"] = hostname
          event["path"] = path

          queue << event
        end
      elsif type == :progressdb
        @codec_plain.decode(data) do |event|

          decorate(event)
          event["host"] = hostname
          event["path"] = path
          event["type"] = "progressdb";

          queue << event
        end        
      end # if
    end # subscribe
    finished
  end # def run

  public
  def teardown
    @tail.sincedb_write
    @tail.quit
  end # def teardown
end # class LogStash::Inputs::TriggeredPackage
