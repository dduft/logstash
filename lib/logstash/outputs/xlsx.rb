require "logstash/outputs/base"
require "logstash/namespace"
require "rubyXL"

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

	    cells = output.split(/ /)

	    wsname = event["wsname"]

      workbook = open(path, wsname)
      index_worksheet = get_worksheet(workbook, wsname)
	    if index_worksheet == -1
        worksheet = RubyXL::Worksheet.new(workbook, wsname)
        workbook.worksheets << worksheet
	    else
	      worksheet = workbook.worksheets[index_worksheet]
	    end

	    row_index = worksheet.sheet_data.size == 1 && worksheet.sheet_data[0][0].nil? ? 0 : worksheet.sheet_data.size

	    cells.each_with_index do |cell, index|
        worksheet.add_cell(row_index, index, cell)
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
  def open(path, wsname)
      return @files[path] if @files.include?(path) and not @files[path].nil?
      @logger.info("Opening file", :path => path)

      dir = File.dirname(path)
      if !Dir.exists?(dir)
        @logger.info("Creating directory", :directory => dir)
        FileUtils.mkdir_p(dir) 
      end
      workbook = RubyXL::Workbook.new
      workbook[0].sheet_name = wsname
      @files[path] = workbook
  end

  public
  def flush(path)
    @files.each do |path, workbook|
      if(File.basename(path) == File.basename(path))
        workbook.write path
      end
    end    
  end

  private
  def get_worksheet(workbook, name)
    workbook.worksheets.each_with_index do |worksheet, index|
      if worksheet.sheet_name == name
        return index
      end
    end
    return -1;
  end
end # class LogStash::Outputs::Xls
