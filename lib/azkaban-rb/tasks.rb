require 'httpclient'

module Azkaban
    
  def self.deploy(uri, path, zip_file)
    client = HTTPClient.new

    client.send_timeout = 1200

    File.open(zip_file) do |file|
      body = { 'path' => path, 'file' => file }
      puts "Uploading jobs ZIP file to #{uri}"
      result = client.post(uri,body)

      # We expect to be redirected after uploading
      raise "Error while uploading to Azkaban" if result.status != 302

      location = result.header["Location"][0]

      success = /Installation Succeeded/
      failure = /Installation Failed:\s*(.+)/

      if location =~ success
        puts "Successfully uploaded to Azkaban"
      elsif (m = failure.match(location))
        reason = m[1]
        raise "Failed to upload to Azkaban: #{reason}"
      else
        raise "Failed to upload to Azkaban for unknown reason"
      end     
    end
  end

  # custom MIME type handler so ZIP is uploaded as 'application/zip' as required by Azkaban UI
  def self.mime_type_handler(path)
    case path
    when /\.txt$/i
      'text/plain'
    when /\.zip$/i
      'application/zip'
    when /\.(htm|html)$/i
      'text/html'
    when /\.doc$/i
      'application/msword'
    when /\.png$/i
      'image/png'
    when /\.gif$/i
      'image/gif'
    when /\.(jpg|jpeg)$/i
      'image/jpeg'
    else
      'application/octet-stream'
    end
  end

  # register our custom MIME handler
  HTTP::Message.mime_type_handler = Proc.new { |path| Azkaban::mime_type_handler(path) }

  class JobFile

    @output_dir = "conf/"

    def initialize(task, ext)
      @task = task
      @ext = ext
      @args = {}
      @read_locks = []
      @write_locks = []
    end

    class << self
      attr_accessor :output_dir
    end

    def set(params)
      params.each do |k,v|
        @args[k] = v
      end
    end

    def reads(name, *options)    
      @read_locks << name
      handle_read_write_options(options, name)
    end
    
    def writes(name, *options)
      @write_locks << name
      handle_read_write_options(options, name)
    end

    def write
      if @args.size > 0
        file_name = @task.name.gsub(":", "-") + @ext
        if @task.prerequisites.size > 0
          scope = @task.scope.map { |s| s.to_s }.join("-")
          @args["dependencies"] = @task.prerequisites.map{ |p| 
            # look up the prerequisite in the scope of its task
            prereq_task = Rake.application.lookup(p, @task.scope)
            prereq_task.name.gsub(":", "-")
          }.join(",")
        end
        create_properties_file(file_name, @args)
        puts "Created #{file_name}"
      end
    end

    private 

    def handle_read_write_options(options, name)
      options = options[0] if options.size > 0
      if options && options.instance_of?(Hash) && options[:as]
        set "param.#{options[:as]}" => name
      end
    end

    def create_properties_file(file_name, props)
      unless File.exists? Azkaban::JobFile.output_dir
        Dir.mkdir Azkaban::JobFile.output_dir
      end
      file = File.new(Azkaban::JobFile.output_dir + file_name, "w+")
      if @read_locks && @read_locks.size > 0
        file.write("read.lock=#{@read_locks.join(",")}\n")
      end
      if @write_locks && @write_locks.size > 0
        file.write("write.lock=#{@write_locks.join(",")}\n")
      end
      props.each do |k,v|
        file.write("#{k}=#{v}\n")
      end
      file.close
    end
  end

end

def props(*args, &b)
  task(*args) do |t|
    unless b.nil?
      job = Azkaban::JobFile.new(t, ".properties")
      job.instance_eval(&b)
      job.write
    end
  end
end

def job(*args,&b)
  task(*args) do |t|
    unless b.nil?
      job = Azkaban::JobFile.new(t, ".job")
      job.instance_eval(&b)
      job.write
    end
  end
end