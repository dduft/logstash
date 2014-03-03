require "logstash/namespace"
require "logstash/outputs/base"

# File output.
#
# Write events to files on disk. You can use fields from the
# event as parts of the filename.
class LogStash::Outputs::Progress < LogStash::Outputs::Base

  config_name "progress"
  milestone 1


  # The format to use when writing events to the file. This value
  # supports any string and can include %{name} and other dynamic
  # strings.
  #
  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  # Where to write the progress database (keeps track of the current
  # position of monitored log files). The default will write
  # progressdb files to some path matching "$HOME/.sincedb*"
  config :progressdb_path, :validate => :string


  public
  def register
    require "fileutils" # For mkdir_p

    workers_not_supported
    @sincedb = {}
  end # def register

  public
  def receive(event)
    return unless output?(event)

    if @message_format
      output = event.sprintf(@message_format)
    else
      output = event.to_json
    end

    ino, dev_major, dev_minor, size, pos = event["message"].split(" ", 5)

    inode = [ino.to_i, dev_major.to_i, dev_minor.to_i, size.to_i]

    if event.include? "tags" and event["tags"].include?("del")
      @sincedb.delete(inode)
    else
      @sincedb[inode] = output
    end
    _sincedb_write(event["path"])
    
  end # def receive

  private
  def _sincedb_write(event_path)
    path = @progressdb_path
    tmp = "#{path}.new"
    begin
      db = File.open(tmp, "w")
    rescue => e
      @logger.warn("_sincedb_write failed: #{tmp}: #{e}")
      return
    end

    @sincedb.each do |inode, message|
      db.puts([message].flatten.join(" "))
    end
    db.close

    begin
      File.rename(tmp, path)
    rescue => e
      @logger.warn("_sincedb_write rename/sync failed: #{tmp} -> #{path}: #{e}")
    end
  end # def _sincedb_write  

  def teardown
    @logger.debug("Teardown: closing files")
    finished
  end
end # class LogStash::Outputs::File


