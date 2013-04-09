# Copyright 2010 LinkedIn, Inc
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'httpclient'
require 'uri'
require 'rest-client'

module Rake
  class Task
    attr_accessor :job
  end
end

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

  # Azkaban2 requires authentication
  def self.auth(uri, username, password)
    uri = URI.parse(uri)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    body = { 'action' => 'login', 'username' => username, 'password' => password }
    request = Net::HTTP::Post.new(uri.request_uri)
    request.set_form_data(body)
    result = http.request(request)
    body = JSON.parse(result.body)
    if body["status"] == "success"
      puts "Successfully authenticated to Azkaban2"
      return body["session.id"]
    else
      raise "Failed to authenticate to Azkaban2"
      return nil
    end
  end
  
  # Azkaban 2 requires a project with project_name to already be created
  def self.deploy_azkaban2(uri, project_name, zip_file, session_id)
    File.open(zip_file) do |file|
      body = { 'action' => 'upload', 'session.id' => session_id, 'project' => project_name, 'file' => file }
      RestClient.post uri, body, :raw_response => true do |response, request, result, &block|
        # Response messages will soon change, and this section will be updated
        set_cookie_header = response.headers[:set_cookie]
        set_cookie_data = set_cookie_header == nil ? "" : set_cookie_header[0].to_s
        if set_cookie_data =~ /azkaban.failure.message/
          raise "Failed to upload to Azkaban: #{set_cookie_data}"
        else
          puts "Successfully uploaded to Azkaban"
        end     
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
    attr_reader :read_locks, :write_locks, :task, :uses_arg, :jvm_args

    @default_jvm_args = {}
    @output_dir = "conf/"

    def initialize(task, ext)
      task.job = self
      @task = task
      @ext = ext
      @args = {}
      @jvm_args = nil
      @read_locks = []
      @write_locks = []
    end

    class << self
      attr_accessor :output_dir
      attr_accessor :default_jvm_args
    end
    
    def [](k)
      @args[k]
    end
    
    def []=(k,v)
      @args[k] = v
    end

    def set(params)
      params.each do |k,v|
        @args[k] = v
      end
    end

    def init_jvm_args
      @jvm_args ||= Azkaban::JobFile.default_jvm_args.clone
    end

    def set_jvm(params)
      init_jvm_args
      params.each do |k,v|
        @jvm_args[k] = v
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

    def caches(filename, options)
      set_jvm "mapred.create.symlink" => "yes"
      reads filename
      symlink = options[:as]
      cache_link = "#{filename}##{symlink}"
      init_jvm_args
      if @jvm_args.has_key?('mapred.cache.files')
        @jvm_args['mapred.cache.files'] += ","+cache_link
      else 
        set_jvm 'mapred.cache.files' => cache_link
      end
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
        if @args["jvm.args"] and not @jvm_args.nil?
          puts "WARNING: #{@task.name} both set 'jvm.args' and set_jvm have been referenced, using: set 'jvm.args' => #{@args["jvm.args"]}"
        end
        if @jvm_args and @args["jvm.args"].nil?
          @args["jvm.args"] = codified_jvm_args
        end
        create_properties_file(file_name, @args)
        puts "Created #{file_name}"
      end
    end

    def codified_jvm_args      
      @jvm_args.map { |key,value| "-D#{key}=#{value}"}.join(" ")
    end

    private 

    def handle_read_write_options(options, name)
      # nothing to do
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
  
  class PigJob < JobFile
    attr_reader :parameters
    
    def initialize(task, ext)
      super(task,ext)
      set "type"=>"pig"
      @parameters = {}
    end
    
    def uses(name)
      @uses_arg = name
      set "pig.script"=>name
    end
    
    def parameter(params)
      params.each do |k,v|
        set "param.#{k}" => v
        @parameters[k] = v
      end
    end

    def handle_read_write_options(options, name)
      options = options[0] if options.size > 0
      if options && options.instance_of?(Hash) && options[:as]
        # set the pig parameter
        set "param.#{options[:as]}" => name
        @parameters[options[:as]] = name
      end
    end
  end
  
  class JavaJob < JobFile
    def initialize(task, ext)
      super(task,ext)
      set "type"=>"java"
    end
    
    def uses(name)
      @uses_arg = name
      set "job.class"=>name
    end

    def handle_read_write_options(options, name)
      options = options[0] if options.size > 0
      if options && options.instance_of?(Hash) && options[:as]
        # set the parameter
        set "#{options[:as]}" => name
      end
    end
  end

  class VoldemortBuildAndPushJob < JobFile    
    def initialize(task, ext)
      super(task,ext)
      set "type"=>"VoldemortBuildAndPush"
    end
  end

  class JavaProcessJob < JobFile    
    def initialize(task, ext)
      super(task,ext)
      set "type"=>"java"
    end
    
    def uses(name)
      @uses_arg = name
      set "java.class"=>name
    end
  end

  class CommandJob < JobFile    
    def initialize(task, ext)
      super(task,ext)
      set "type"=>"command"
    end
    
    def uses(text)
      @uses_arg = text
      set "command"=>text
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
  make_job(Azkaban::JobFile, args, b)
end

def pig_job(*args,&b) 
  make_job(Azkaban::PigJob, args, b)
end

def java_job(*args,&b) 
  make_job(Azkaban::JavaJob, args, b)
end

def java_process_job(*args,&b) 
  make_job(Azkaban::JavaProcessJob, args, b)
end

def command_job(*args,&b) 
  make_job(Azkaban::CommandJob, args, b)
end

def voldemort_build_and_push(*args,&b)
  make_job(Azkaban::VoldemortBuildAndPushJob, args, b)
end

def make_job(job_class,args,b)
  job = job_class.new(task(*args) { job.write }, ".job")
  unless b.nil?    
    job.instance_eval(&b)
  end
end
