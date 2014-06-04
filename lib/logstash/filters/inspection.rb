require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::Inspection < LogStash::Filters::Base

    config_name "inspection"
    milestone 1

    # Inspection interval in secends
    config :inspection_interval, :validate => :number, :default => 600

    # Tag which be added to the event every <inspection_interval> seconds
    config :inspection_tag, :validate => :string, :default => "inspection"

    public
    def register

        now = Time.now
        @last_inspection_run = now
    end #def register

    public
    def filter(event)
        check_inspection_interval(event)
    end # def filter

    private
    def check_inspection_interval(event)
        now = Time.now
        return unless now - @last_inspection_run >= @inspection_interval

        run_inspections(event)

        @last_inspection_run = now
    end

    def run_inspections(event)
        event.tag(@inspection_tag)
    end
end # class LogStash::Filters::Inspection
