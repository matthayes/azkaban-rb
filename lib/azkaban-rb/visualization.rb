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

require 'graphviz_r'

class RakeGraph
  attr_reader :tasks
  
  def initialize(namespaces = nil)
    @namespaces = namespaces
    @tasks = {}
    Rake.application.tasks.find_all{ |task| (not task.job.nil?)}.each do |task|
      tasks[RakeGraph.task_name(task)] = task if (task.job.read_locks.size + task.job.write_locks.size) > 0
    end
    @nodes = {}
    @edges = []
    construct_graph()
  end
  
  def RakeGraph.task_name(task)
    task_name = "TASK#{task}"
    task_name = task_name.gsub(/[^0-9a-z ]/i, '')
    return task_name
  end
  
  def RakeGraph.data_name(name)
    name = "DATA"+name.gsub(/[^0-9a-z ]/i, '')
    return name
  end
  
  def task_in_namespace(task)
    return true if @namespaces.nil? or @namespaces.size == 0
    return (task.scope & @namespaces).size > 0
  end
  
  def find_prereq(task, prereq)    
    scopes = Array.new(task.scope)
    while prereq.start_with? '^'
      scopes.pop
      prereq.slice!(0)
    end
    return RakeGraph.task_name(scopes.join('')+prereq)
  end
  
  def construct_graph()
    # first add all of the task nodes
    @tasks.each do |task_name, task|
      next unless task_in_namespace(task)        
      node = TaskNode.new(task)
      @nodes[node.name] = node
    end
    
    # now add all of the edges and data nodes    
    data_nodes = {}
    @nodes.each do |name, node|
      task = node.task
      # find all prereq tasks
      # task.prerequisites.each do |prereq|
      #   prereq = find_prereq(task, prereq)
      #   next unless @nodes.has_key?(prereq)
      #   @edges << TaskEdge.new(prereq, node.name)
      # end
      # find all data reads
      task.job.read_locks.each do |read_lock|
        data_name = RakeGraph.data_name(read_lock)
        data_nodes[data_name] = DataNode.new(read_lock) unless data_nodes.has_key? data_name
        @edges << DataEdge.new(data_name, node.name)
      end
      # find all data writes
      task.job.write_locks.each do |write_lock|
        data_name = RakeGraph.data_name(write_lock)
        data_nodes[data_name] = DataNode.new(write_lock) unless data_nodes.has_key? data_name
        @edges << DataEdge.new(node.name, data_name)
      end
    end    
    data_nodes.each do |key, value|
      @nodes[key] = value
    end
  end
  
  class Node
    attr_reader :name, :type
    
    def initialize(name, type)
      @name = name
      @type = type
    end
    
    def fontcolor
      return '#000000'
    end
  
    def to_s
      return "#{@type}: #{@name}"
    end
  end
  
  class TaskNode < Node
    attr_reader :task
    
    def initialize(task)
      super(RakeGraph.task_name(task), task.job.class.to_s)
      @task = task
    end
    
    def label
      label = "<#{@task}<br/>#{@task.job.uses_arg}>"
      return label.to_sym
    end
    
    def shape
      return :ellipse
    end
    
    def fillcolor
      case @type
        when 'Azkaban::PigJob'
          return '#e7a5a5'
        when 'Azkaban::JavaJob'
          return '#E7C6A5'
        when 'Azkaban::CommandJob'
          return '#e7e6a5'
      end
      return ""
    end
  end
  
  class DataNode < Node
    attr_reader :filename
    
    def initialize(filename)
      super(RakeGraph.data_name(filename), "data")
      @filename = filename
    end
    
    def label
      label = @filename
      return "<#{label}>".to_sym
    end
    
    def shape
      return :box
    end
    
    def fillcolor
      return '#d2e3f3'
    end
  end
  
  class Edge
    attr_reader :source, :dest, :type
    
    def initialize(source, dest)
      @source = source
      @dest = dest
    end
    
    def to_s
      return "#{source} >> #{dest}"
    end
  end
  
  class TaskEdge < Edge
    def initialize(source, dest)
      super(source, dest)
      @type = "task"
    end
    
    def style
      return :dotted
    end
  end
  
  class DataEdge < Edge
    def initialize(source, dest)
      super(source, dest)
      @type = "data"
    end
    
    def style
      :solid
    end
  end
  
  def visualize(name, output_file)
    g = GraphvizR.new name
    g.graph[:label => name]
    add_nodes(g)
    add_edges(g)
    g.output output_file
  end
  
  def add_nodes(g)
    @nodes.each do |name, node|
      g[name] [:label => @label_block.nil? ? node.label : @label_block.call(node), 
        :shape => @shape_block.nil? ? node.shape : @shape_block.call(node),
        :fillcolor => @fillcolor_block.nil? ? node.fillcolor : @fillcolor_block.call(node),
        :style => :filled, 
        :fontcolor => @fontcolor_block.nil? ? node.fontcolor : @fontcolor_block.call(node)]
    end
  end
  
  def add_edges(g)
    @edges.each do |edge|
      (g[edge.source]>>g[edge.dest])[:style => edge.style]
    end
  end
  
  def set_label(&block)
    @label_block = block
  end
  
  def set_fillcolor(&block)
    @fillcolor_block = block
  end
  
  def set_fontcolor(&block)
    @fontcolor_block = block
  end
  
  def set_shape(&block)
    @shape_block = block
  end
end
