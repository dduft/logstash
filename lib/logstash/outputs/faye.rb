require "logstash/namespace"
require "logstash/outputs/base"

# File output.
#
# Write events to files on disk. You can use fields from the
# event as parts of the filename.
class LogStash::Outputs::Faye < LogStash::Outputs::Base

  config_name "faye"
  milestone 1

  # Faye-channel on to be sent on
  config :channel, :validate => :string

  # Faye-security-token
  config :faye_token, :validate => :string

  # Url of the faye-server
  config :faye_url, :validate => :string, :default => "http://localhost:9292/faye"

  public
  def register
    require "net/http"
  end # def register

  public
  def receive(event)
    return unless output?(event)

  _send_message event
  end # def receive

  private
  def _send_message(data)
    if @faye_token
      message = {:channel => @channel, :data => data, :ext => {:auth_token => @faye_token}}
    else
      message = {:channel => @channel, :data => data}
    end

    uri = URI.parse(@faye_url)
    res = Net::HTTP.post_form(uri, :message => message.to_json)

    if res.is_a?(Net::HTTPSuccess)
      message = JSON.parse(res.body)

      @logger.warn("Faye request was not successfully", message.first) unless message.first['successful']
    else
      @logger.warn("Faye response not ok", :http_status => res.code, :message => res.message)
    end

  end

  def teardown
    finished
  end
end # class LogStash::Outputs::Faye


