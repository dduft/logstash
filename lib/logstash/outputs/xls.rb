require "logstash/outputs/base"
require "logstash/namespace"
require "spreadsheet"

# A null output. This is useful for testing logstash inputs and filters for
# performance.
class LogStash::Outputs::Xls < LogStash::Outputs::Base
  config_name "xls"
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

	    workbook = open(path)

	    if event.is_a? LogStash::Event and @message_format
	      output = event.sprintf(@message_format)
	    else
	      output = event["message"]
	    end

	    cells = output.split(/;/)

	    wsname = event["wsname"]

	    if(workbook.worksheet(wsname).nil?)
	      sheet = workbook.create_worksheet :name => wsname
	    else
	      sheet = workbook.worksheet wsname
	    end

	    row_index = sheet.row_count == 0 ? 0 : sheet.last_row_index + 1
	    cells.each do |cell|
	      sheet.row(row_index).push cell
	    end
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

      @files[path] = Spreadsheet::Workbook.new
  end

  public
  def flush(path)
    @files.each do |path, workbook|
      if(File.basename(path) == File.basename(path))
        workbook.write path
      end
    end    
  end  
end # class LogStash::Outputs::Xls
