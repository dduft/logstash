require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::PackagedSqlite < LogStash::Outputs::Base

    config_name "packagedsqlite"
    milestone 1

    # The path to the sqlite database file.
    config :path, :validate => :string, :required => true

    # What type does the triggers have, which were sent from the input e.g. triggeredpackage
    config :trigger_type, :validate => :string, :default => "trigger"

    # The attribute/field, where the the timestamp for triggers come from
    config :triggertime_attribute, :validate => :string, :default => "triggertime"

    # The attribute/field, where the the timespan for triggers come from
    config :timespan_attribute, :validate => :string, :default => "timespan"

    # For every filter-type, a seperate filter will be generated 
    # This depends on the log-types in the pipeline
    config :filter_types, :validate => :array, :required => true

    # Where to write packages in the given db
    # The second parameter indicates, if singularization is neccessary,
    # when generating the foreign-key-name for the table
    # e.g. table-name is packages. The foreign-key-name would be packages_id
    # With singularization = true, the generated foreign-key-name is package_id
    config :table_packages, :validate => :array, :default => ["packages", "true"]

    # Where to write triggers in the given db
    # The second parameter indicates, if singularization is neccessary,
    # when generating the foreign-key-name for the table
    # e.g. table-name is packages. The foreign-key-name would be packages_id
    # With singularization = true, the generated foreign-key-name is package_id
    config :table_triggers, :validate => :array, :default => ["triggers", "true"]

    # Where to write filters in the given db
    # The second parameter indicates, if singularization is neccessary,
    # when generating the foreign-key-name for the table
    # e.g. table-name is packages. The foreign-key-name would be packages_id
    # With singularization = true, the generated foreign-key-name is package_id
    config :table_filters, :validate => :array, :default => ["filters", "true"]

    # Where to write zot in the given db
    # The second parameter indicates, if singularization is neccessary,
    # when generating the foreign-key-name for the table
    # e.g. table-name is packages. The foreign-key-name would be packages_id
    # With singularization = true, the generated foreign-key-name is package_id
    config :table_zot, :validate => :array, :default => ["markables", "true"]


    private
    def set_table_meta
        @tables["packages"] = DBTable.new(@config["table_packages"].shift, @config["table_packages"])
        @tables["triggers"] = DBTable.new(@config["table_triggers"].shift, @config["table_triggers"])
        @tables["filters"] = DBTable.new(@config["table_filters"].shift, @config["table_filters"])
        @tables["zot"] = DBTable.new(@config["table_zot"].shift, @config["table_zot"])
    end

    private
    def get_all_tables(db)
        return db["SELECT * FROM sqlite_master WHERE type = 'table' AND tbl_name NOT LIKE 'sqlite_%'"]
                .map { |t| t[:name] }
    end

    private
    def check_tables(tables)
        if !tables.include?(@tables["packages"].name)
            raise ArgumentError.new("Could not find table #{@tables["packages"].name} in db")
        end
        if !tables.include?(@tables["triggers"].name)
            raise ArgumentError.new("Could not find table #{@tables["triggers"].name} in db")
        end
        if !tables.include?(@tables["filters"].name)
            raise ArgumentError.new("Could not find table #{@tables["filters"].name} in db")
        end                
        if !tables.include?(@tables["zot"].name)
            raise ArgumentError.new("Could not find table #{@tables["zot"].name} in db")
        end
    end

    private
    def insert_package(db, title)
        begin
            @logger.debug("insert package") if @logger.debug? 
            packages = db[@tables["packages"].nameSym]
            id = packages.max(:rowid).nil? ? 0 : packages.max(:rowid)
            now = to_rails_format(Time.new)
            title = remove_(title)

            package = packages.first(:title => "#{title}")
            if package.nil? || package.count == 0
                packages.insert(:id => id+1,
                                :title => title,
                                :created_at => now, 
                                :updated_at => now)
                return id+1
            else
                return package[:id]
            end
        rescue => e
            @logger.warn("Could not write to package: #{e}")
        end
    end

    private
    def insert_trigger(db, trigger_event, package_id)
        begin
            @logger.debug("insert trigger") if @logger.debug? 
            triggers = db[@tables["triggers"].nameSym]
            id = triggers.max(:rowid).nil? ? 0 : triggers.max(:rowid)
            now = to_rails_format(Time.new)


            name = remove_("trigger_#{trigger_event[:triggertime]}")

            trigger = triggers.first(   :name => name, 
                                        @tables["packages"].foreignSym => package_id)

            startTime = trigger_event[:timestamp]
            startTime -= trigger_event[:timespan].to_i

            endTime = trigger_event[:timestamp] 
            endTime += trigger_event[:timespan].to_i
            endTime -= 0.001 #minus 1 ms

            if trigger.nil? || trigger.count == 0
                triggers.insert(:id => id+1,
                                :name => name,
                                :from => to_rails_format(startTime),
                                :to => to_rails_format(endTime),
                                @tables["packages"].foreignSym => package_id,
                                :created_at => now, 
                                :updated_at => now)
                yield id+1
            else
                yield trigger[:id]
            end
        rescue => e
            @logger.warn("Could not write to trigger: #{e}")
        end
    end

    private
    def insert_filter(db, package_id, tag)
        begin
            @logger.debug("insert filter") if @logger.debug?
            filters = db[@tables["filters"].nameSym]
            id = filters.max(:rowid).nil? ? 0 : filters.max(:rowid)
            now = to_rails_format(Time.new)
            name = "#{tag} *"

            filter = filters.first( :name => name, 
                                     @tables["packages"].foreignSym => package_id)

            if filter.nil? || filter.count == 0
                filters.insert( :id => id+1,
                                @tables["packages"].foreignSym => package_id,
                                :name => name,
                                :query => "*",
                                :tags => tag,
                                :created_at => now, 
                                :updated_at => now)
                return id+1
            else
                return filter[:id]
            end
        rescue => e
            @logger.warn("Could not write to insert_filter: #{e}")
        end            
    end    

    private
    def insert_zot(db, filter_id, trigger_id, pos)
        begin
            @logger.debug("insert zot") if @logger.debug? 
            zots = db[@tables["zot"].nameSym] #sorry for the name :-)
            id = zots.max(:rowid).nil? ? 0 : zots.max(:rowid)
            now = to_rails_format(Time.new)

            zot = zots.first(   @tables["filters"].foreignSym => filter_id, 
                                @tables["triggers"].foreignSym => trigger_id)        

            if zot.nil? || zot.count == 0
                zots.insert( :id => id+1,
                            @tables["filters"].foreignSym => filter_id,
                            @tables["triggers"].foreignSym => trigger_id,
                            :position => pos,
                            :created_at => now, 
                            :updated_at => now)

                return id+1
            else
                return zot[:id]
            end
        rescue => e
            @logger.warn("Could not write to zot: #{e}")
        end            
    end

    private
    def to_rails_format(time)
      return time.strftime("%Y-%m-%d %H:%M:%S.%6N")
    end

    private
    def remove_(string)
        string.gsub "_", " "
    end

    public
    def register
        require "sequel"
        require "jdbc/sqlite3"
        
        @tables={}

        @host = Socket.gethostname
        @logger.info("Registering sqlite output", :database => @path)
        @db = Sequel.connect("jdbc:sqlite:#{@path}")
        set_table_meta
        tables = get_all_tables(@db)
        check_tables(tables)
    end # def register

    public
    def receive(event)
        return if !output?(event) || event["tags"].include?("del")

        trigger_event = {
            :timestamp => event.timestamp,
            :triggertime => event[@triggertime_attribute],
            :timespan => event[@timespan_attribute]
        }
        package = event["package"]
        @logger.debug("Trigger from package received", :package => package) if @logger.debug?

        package_id = insert_package(@db, package)

        insert_trigger(@db, trigger_event, package_id) do |trigger_id|
            @filter_types.each_with_index do |tag, pos|
                filter_id = insert_filter(@db, package_id, tag)
                insert_zot(@db, filter_id, trigger_id, pos)          
            end               
        end
    end # def receive
end # class LogStash::Outputs::PackagedSqlite

class DBTable
    def initialize(name, pluralized)
        @name = name
        @pluralized = pluralized
    end
    def name
        @name
    end
    def nameSym
        @name.to_sym
    end    
    def foreignSym
        if(@pluralized)
            "#{@name.chop}_id".to_sym
        else
            "#{@name}_id".to_sym
        end
    end
end
