# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "pry"

class LogStash::Outputs::Http2 < LogStash::Outputs::Base
    # This output lets you `PUT` or `POST` events to a
    # generic HTTP(S) endpoint
    #
    # Additionally, you are given the option to customize
    # the headers sent as well as basic customization of the
    # event json itself.

    config_name "http2"
    milestone 1

    #If sign in is supposed
    config :sign_in, :validate => :boolean, :default => false
    config :sign_in_params, :validate => :hash
    config :host, :validate => :string
    config :port, :validate => :number
    config :path, :validate => :string
    config :root_path, :validate => :string, :default => "/"
    config :sign_in_path, :validate => :string, :default => "/users/sign_in"

    # URL to use
    config :url, :validate => :string

    # validate SSL?
    config :verify_ssl, :validate => :boolean, :default => true

    # What verb to use
    # only put and post are supported for now
    config :http_method, :validate => ["put", "post"], :required => :true

    # Custom headers to use
    # format is `headers => ["X-My-Header", "%{host}"]
    config :headers, :validate => :hash

    # Content type
    #
    # If not specified, this defaults to the following:
    #
    # * if format is "json", "application/json"
    # * if format is "form", "application/x-www-form-urlencoded"
    config :content_type, :validate => :string

    # This lets you choose the structure and parts of the event that are sent.
    #
    #
    # For example:
    #
    #    mapping => ["foo", "%{host}", "bar", "%{type}"]
    config :mapping, :validate => :hash

    # Set the format of the http body.
    #
    # If form, then the body will be the mapping (or whole event) converted
    # into a query parameter string (foo=bar&baz=fizz...)
    #
    # If message, then the body will be the result of formatting the event according to message
    #
    # Otherwise, the event is sent as json.
    config :format, :validate => ["json", "form", "message"], :default => "json"

    config :message, :validate => :string

    public
    def register
        require "ftw"
        require "uri"
        require "securerandom"
        @agent = FTW::Agent.new

        if @content_type.nil?
            case @format
                when "form" ; @content_type = "application/x-www-form-urlencoded"
                when "json" ; @content_type = "application/json"
            end
        end

        if @format == "message"
            if @message.nil?
                raise "message must be set if message format is used"
            end
            if @content_type.nil?
                raise "content_type must be set if message format is used"
            end
            unless @mapping.nil?
                @logger.warn "mapping is not supported and will be ignored if message format is used"
            end
        end

        # if @sign_in.nil?
            # if @url.nil?
                # raise "message must be set if message format is used"
            # end
        # end

        @signed_in = false
        @csrf_token = nil
        @cookies = nil
    end # def register

    public
    def receive(event)
        return unless output?(event)

        sign_in() if @sign_in

        if @mapping
            evt = Hash.new
            @mapping.each do |k,v|
                evt[k] = event.sprintf(v)
            end
        else
            evt = event.to_hash
        end

        @url = URI::HTTP.build({:host => @host, :port=> @port, :path => @path}) if @url.nil?

        case @http_method
        when "put"
          request = @agent.put(event.sprintf(@url))
        when "post"
          request = @agent.post(event.sprintf(@url))
        else
          @logger.error("Unknown verb:", :verb => @http_method)
        end

        if @headers
          @headers.each do |k,v|
            request.headers[k] = event.sprintf(v)
          end
        end

        request["Content-Type"] = @content_type

        set_header(request)

        begin
            if @format == "json"
                request.body = evt.to_json
            elsif @format == "message"
                request.body = event.sprintf(@message)
            else
                request.body = encode(evt)
            end

            response = @agent.execute(request)

        rescue Exception => e
            @logger.warn("Unhandled exception", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
        end
    end # def receive

    private
    def encode(hash)
        return hash.collect do |key, value|
            CGI.escape(key) + "=" + CGI.escape(value)
        end.join("&")
    end # def encode

    def set_cookies(response)
        set_cookie = response.headers['set-cookie']

        if(!set_cookie.nil? && !set_cookie.empty?)
            cookies = []
            set_cookie.each do |c|
                cookie = c.split('; ')[0]

                @csrf_token = CGI.unescape(cookie.split('=')[1]) if cookie.start_with?('XSRF-TOKEN')

                cookies << cookie
            end
            @cookies = cookies.join('; ') if !cookies.empty?
        end
    end

    def sign_in
        if(!@signed_in)
            if(@cookies.nil?)
                request = @agent.get(
                    URI::HTTP.build({:host => @host, :port=> @port, :path => @root_path})
                )
                request["Content-Type"] = @content_type
                response = @agent.execute(request)

                set_cookies(response)
            end

            request = @agent.post(
                URI::HTTP.build({:host => @host, :port=> @port, :path => @sign_in_path})
            )
            request["Content-Type"] = @content_type

            set_header(request)

            body = Hash.new
            @sign_in_params.each do |k,v|
                body[k] = v
            end

            request.body = body.to_json

            response = @agent.execute(request)

            set_cookies(response)

            @signed_in = response.status == 200
        end
    end

    def set_header(request)
        request.headers['Cookie'] = @cookies if !@cookies.nil?

        if !@csrf_token.nil?
          request.headers['X-Requested-With'] = 'XMLHttpRequest'
          request.headers['X-CSRF-Token'] = @csrf_token
          request.headers['X-XSRF-Token'] = @csrf_token
        end
    end
end
