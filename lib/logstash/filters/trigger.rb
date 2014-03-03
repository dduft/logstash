require "logstash/filters/base"
require "logstash/namespace"
require 'date'

class LogStash::Filters::Trigger < LogStash::Filters::Base

    if RUBY_ENGINE == "jruby"
        JavaException = java.lang.Exception
        UTC = org.joda.time.DateTimeZone.forID("UTC")
    end

    config_name "trigger"
    milestone 1

    # specify a timezone canonical ID to be used for date parsing.
    # The valid ID are listed on http://joda-time.sourceforge.net/timezones.html
    # Useful in case the timezone cannot be extracted from the value,
    # and is not the platform default
    # If this is not specified the platform default will be used.
    # Canonical ID is good as it takes care of daylight saving time for you
    # For example, America/Los_Angeles or Europe/France are valid IDs
    config :timezone, :validate => :string

    # Drop events that don't match
    #
    # If this is set to false, no events will be dropped at all. Rather, the
    # requested tags and fields will be added to matching events, and
    # non-matching events will be passed through unchanged.
    config :drop, :validate => :boolean, :default => false

    # The attribute/field, where the matching triggers will be saved
    config :trigger_attribute, :validate => :string, :default => "trigger"

    # The attribute/field, where the the timestamp for triggers come from
    config :timestamp_attribute, :validate => :string, :default => "timestamp"

    # The attribute/field, where the the timespan for triggers come from
    config :timespan_attribute, :validate => :string, :default => "timespan"

    # Default timespan, if no set in trigger-file
    config :timespan_default, :validate => :string, :default => "60"

    # Where should we load the triggers from?
    config :trigger_path, :validate => :string, :default => "Triggers_*"

    # The regular expression to match
    config :trigger_pattern, :validate => :string, :required => true

    # Date-format of the triggers, sent from an input e.g. triggeredpackage
    config :trigger_format, :validate => :string, :required => true    

    # logstash ships by default with a bunch of patterns, so you don't
    # necessarily need to define this yourself unless you are adding additional
    # patterns.
    #
    # Pattern files are plain text with format:
    #
    #     NAME PATTERN
    #
    # For example:
    #
    #     NUMBER \d+
    config :patterns_dir, :validate => :array, :default => []

    public
    def register
        require "grok-pure" # rubygem 'jls-grok'
        # Detect if we are running from a jarfile, pick the right path.
        patterns_path = []
        if __FILE__ =~ /file:\/.*\.jar!.*/
          patterns_path += ["#{File.dirname(__FILE__)}/../../patterns/*"]
        else
          patterns_path += ["#{File.dirname(__FILE__)}/../../../patterns/*"]
        end

        @grok = Grok.new

        @patterns_dir = patterns_path.to_a + @patterns_dir
        @patterns_dir.each do |path|
          # Can't read relative paths from jars, try to normalize away '../'
          while path =~ /file:\/.*\.jar!.*\/\.\.\//
            # replace /foo/bar/../baz => /foo/baz
            path = path.gsub(/[^\/]+\/\.\.\//, "")
          end

          if File.directory?(path)
            path = File.join(path, "*")
          end

          Dir.glob(path).each do |file|
            @logger.info("Grok loading patterns from file", :path => file)
            @grok.add_patterns_from_file(file)
          end
        end

        @grok.compile(@trigger_pattern)


        joda_parser = org.joda.time.format.DateTimeFormat.forPattern(@trigger_format).withDefaultYear(Time.new.year)
        if @timezone
            joda_parser = joda_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
        else
            joda_parser = joda_parser.withOffsetParsed
        end

        @parser = lambda { |date| joda_parser.parseDateTime(date) }


        @triggers = Hash.new { |h,k| h[k] = [] }
        @last_trigger_times = Hash.new { |h,k| h[k] = [] }

        @trigger_cleanup_interval = 10      
    end #def register

    public
    def filter(event)
        matches = 0
        dirname = File.dirname(event["path"])

        cleanup_triggers(event)

        read_triggers(dirname)

        @triggers[dirname].each do |trigger|
            
            startTime = trigger[:timestamp]
            startTime -= trigger[:timespan].to_i

            endTime = trigger[:timestamp]   
            endTime += trigger[:timespan].to_i

            if event.timestamp >= startTime && event.timestamp <= endTime
                event[@trigger_attribute] ||= []
                event[@trigger_attribute] << trigger unless event[@trigger_attribute].include?(trigger)
                matches += 1
            end
        end 

        if matches > 0
          filter_matched(event)
        else
            if @drop == true
                @logger.debug("trigger: dropping event, no matches") if @logger.debug?
                event.cancel
            else
                @logger.debug("trigger: no matches, but drop set to false") if @logger.debug?
            end
        end
    end # def filter

    private
    def read_triggers(dirname)
        return if @triggers.include?(dirname) and not @triggers[dirname].nil?

        @logger.debug("read triggers for dir #{dirname}") if @logger.debug?

        triggerglob = Dir.glob(dirname + '/' + @trigger_path)
        return unless triggerglob.length > 0

        triggerglob.each do |triggerpath|
            File.readlines(triggerpath).each do |line|
                set_triggers(dirname, line.strip! || line)
            end        
        end
    end

    private
    def set_triggers(dirname, line)
        fields = {}

        match = @grok.match(line)

        match.each_capture do |capture, value|
            syntax, semantic, coerce = capture.split(":")
            if !semantic.nil?
                fields[semantic] = value
            end
        end

        jtime = @parser.call(fields[@timestamp_attribute])
        jtime = jtime.withZone(UTC)

        timestamp = Time.utc(
            jtime.getYear, jtime.getMonthOfYear, jtime.getDayOfMonth,
            jtime.getHourOfDay, jtime.getMinuteOfHour, jtime.getSecondOfMinute,
            jtime.getMillisOfSecond * 1000
        )        

        if !fields.include?(@timespan_attribute) or fields[@timespan_attribute].nil?
            timespan = @timespan_default
        else
            timespan = fields[@timespan_attribute]
        end
        
        trigger = { 
            :timestamp => timestamp,
            :timespan => timespan
        }
        
        unless @triggers[dirname].include? trigger
            @logger.debug("@triggers: add trigger", :timestamp => timestamp, :timespan => timespan) if @logger.debug?
            @triggers[dirname] << trigger
            @last_trigger_times[dirname] = Time.now
        end
    end

    # every 10 seconds or so after last trigger(triggered by events, but if there are no events there's no point closing triggers anyway)
    def cleanup_triggers(event)
        now = Time.now
        dirname = File.dirname(event["path"])

        # renew the timestamp
        @last_trigger_times[dirname] = now if @last_trigger_times.include? dirname

        @last_trigger_times.each do |dirname, last_time|
            if now - last_time >= @trigger_cleanup_interval
                @triggers.delete dirname
                @last_trigger_times.delete dirname
            end
        end
    end    
end # class LogStash::Filters::Trigger
