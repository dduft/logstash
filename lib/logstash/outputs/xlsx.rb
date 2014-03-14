require "logstash/outputs/base"
require "logstash/namespace"
require "axlsx"

# A null output. This is useful for testing logstash inputs and filters for
# performance.
class LogStash::Outputs::Xlsx < LogStash::Outputs::Base
  config_name "xlsx"
  milestone 1

  default :codec, "xls"

  # The format to use when writing events to the file. This value
  # supports any string and can include %{name} and other dynamic
  # strings.
  #
  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  # The path to the file to write. Event fields can be used here,
  # like "/var/log/logstash/%{host}/%{application}"
  # One may also utilize the path option for date-based log
  # rotation via the joda time format. This will use the event
  # timestamp.
  # E.g.: path => "./test-%{+YYYY-MM-dd}.txt" to create
  # ./test-2013-05-29.txt
  config :path, :validate => :string

  public
  def register
  	@files = {}

    @codec.on_event do |event|
      if @path
        path = event.sprintf(@path)
      else
        path = event["path"]
      end

	    if event.is_a? LogStash::Event and @message_format
	      output = event.sprintf(@message_format)
	    else
	      output = event["message"]
	    end

	    cells = output.split(/;/)

	    wsname = event["wsname"]

      worksheet = get_worksheet(path, wsname)

      worksheet.add_row cells
    end
  end # def register

  public
  def receive(event)
    @codec.encode(event)

    if event.include? "tags" and event["tags"].include?("eof")
        flush event['path']
    end
  end # def event

  private
  def open(path)
      return @files[path] if @files.include?(path) and not @files[path].nil?
      @logger.info("Opening file", :path => path)

      dir = File.dirname(path)
      if !Dir.exists?(dir)
        @logger.info("Creating directory", :directory => dir)
        FileUtils.mkdir_p(dir)
      end
      package = Axlsx::Package.new
      @files[path] = package
  end

  public
  def flush(path)
    @files.each do |path, package|
      if(File.basename(path) == File.basename(path))
        package.serialize(path)
      end
    end
  end

  private
  def get_worksheet(path, wsname)
    package = open(path)

    worksheet = package.workbook.sheet_by_name(wsname)

    if(worksheet.nil?)
      package.workbook.add_worksheet(:name => wsname) do |ws|
        return ws
      end
    else
      return worksheet
    end
  end
end # class LogStash::Outputs::Xls
